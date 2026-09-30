"""Generates three customer exports of roughly equal size (CSV, JSON Lines, YAML) from a fixed seed.

Each file imitates a different source system with its own field names and conventions:
  crm_customers.csv        - ';'-separated, DD.MM.YYYY dates, decimal comma, Y/N flags, English country names
  webshop_customers.jsonl  - one JSON object per line, camelCase keys, nested address, ISO dates, country codes
  legacy_members.yml       - multi-document YAML stream, "Last, First" names, lowercase native country names
Some rows are dirty on purpose (dirty_ratio) and some customers appear in more than one source (overlap_ratio).
The same seed always produces the same files, so only this script needs to be committed, not the data.
"""
import argparse
import csv
import json
import random
import unicodedata
import zipfile
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

from reference_data import COUNTRIES, CRM_FILE, EMAIL_DOMAINS, LEGACY_FILE, MANIFEST_FILE, WEBSHOP_FILE

DEFECTS = (
    "blank_email", "invalid_email", "padded_name", "invalid_birth_date",
    "country_spelling", "bad_lifetime_value", "negative_lifetime_value", "duplicate_row",
)
INVALID_DATE_TEXTS = ("unknown", "", "00.00.0000", "1990-02-31", "n/a")
FIRST_BIRTH_DAY = date(1940, 1, 1).toordinal()
LAST_BIRTH_DAY = date(2007, 12, 31).toordinal()
FIRST_REGISTRATION = datetime(2015, 1, 1)
REGISTRATION_SPAN_SECONDS = int((datetime(2026, 6, 30) - FIRST_REGISTRATION).total_seconds())
SIZE_CHECK_INTERVAL = 1000
LETTERS_WITHOUT_DECOMPOSITION = str.maketrans({"ł": "l", "Ł": "L"})


def ascii_fold(text: str) -> str:
    """'Łukasz Wiśniewski' -> 'lukaszwisniewski' (for building e-mail addresses)."""
    decomposed = unicodedata.normalize("NFKD", text.translate(LETTERS_WITHOUT_DECOMPOSITION))
    return "".join(character for character in decomposed if character.isascii() and character.isalnum()).lower()


def yaml_quote(text: str) -> str:
    """A JSON string literal is also a valid double-quoted YAML scalar."""
    return json.dumps(text, ensure_ascii=False)


def date_text(value, date_format: str) -> str:
    """Dirty values are already strings and are written as they are."""
    return value if isinstance(value, str) else value.strftime(date_format)


def money_text(value, decimal_separator: str) -> str:
    if isinstance(value, str):
        return value
    sign = "-" if value < 0 else ""
    whole, cents = divmod(abs(value), 100)
    return f"{sign}{whole}{decimal_separator}{cents:02d}"


class DataGenerator:
    def __init__(self, output_dir: Path, target_megabytes: float = 200, seed: int = 42,
                 dirty_ratio: float = 0.02, overlap_ratio: float = 0.05,
                 yaml_batch_size: int = 5000, overlap_pool_size: int = 20_000):
        self.output_dir = Path(output_dir)
        self.target_bytes = int(target_megabytes * 1024 * 1024)
        self.seed = seed
        self.dirty_ratio = dirty_ratio
        self.overlap_ratio = overlap_ratio
        self.yaml_batch_size = yaml_batch_size
        self.overlap_pool_size = overlap_pool_size

        self.random = random.Random(seed)
        self.next_customer_id = 1
        self.customers_from_previous_sources = []
        self.current_source_sample = []
        self.current_source_new_customers = 0
        self.source_reports = {}

    # ---------- customers ----------

    def new_customer(self) -> dict:
        country = self.random.choice(COUNTRIES)
        first_name = self.random.choice(country.first_names)
        last_name = self.random.choice(country.last_names)
        customer_id = self.next_customer_id
        self.next_customer_id += 1
        return {
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{ascii_fold(first_name)}.{ascii_fold(last_name)}{customer_id}@{self.random.choice(EMAIL_DOMAINS)}",
            "phone": f"+{country.phone_prefix} {self.random.randint(100, 999)} "
                     f"{self.random.randint(100, 999)} {self.random.randint(100, 999)}",
            "birth_date": date.fromordinal(self.random.randint(FIRST_BIRTH_DAY, LAST_BIRTH_DAY)),
            "street": f"{self.random.choice(country.streets)} {self.random.randint(1, 250)}",
            "city": self.random.choice(country.cities),
            "postal_code": self.postal_code(country.postal_pattern),
            "country": country,
            "country_text": None,
            "registered_at": FIRST_REGISTRATION + timedelta(seconds=self.random.randrange(REGISTRATION_SPAN_SECONDS)),
            "is_active": self.random.random() < 0.8,
            "lifetime_value": self.random.randint(0, 5_000_000),  # in cents
        }

    def postal_code(self, pattern: str) -> str:
        characters = []
        for symbol in pattern:
            if symbol == "#":
                characters.append(str(self.random.randint(0, 9)))
            elif symbol == "@":
                characters.append(chr(self.random.randint(ord("A"), ord("Z"))))
            else:
                characters.append(symbol)
        return "".join(characters)

    def remember_for_overlap(self, customer: dict):
        """Reservoir sampling: keeps a fixed-size random sample of this source's customers."""
        self.current_source_new_customers += 1
        if len(self.current_source_sample) < self.overlap_pool_size:
            self.current_source_sample.append(customer)
        else:
            replaced_index = self.random.randrange(self.current_source_new_customers)
            if replaced_index < self.overlap_pool_size:
                self.current_source_sample[replaced_index] = customer

    def apply_defect(self, record: dict, defect: str):
        if defect == "blank_email":
            record["email"] = ""
        elif defect == "invalid_email":
            record["email"] = record["email"].replace("@", self.random.choice(("#", " at ", "")))
        elif defect == "padded_name":
            record["first_name"] = f"  {self.random.choice((str.upper, str.lower))(record['first_name'])} "
        elif defect == "invalid_birth_date":
            record["birth_date"] = self.random.choice(INVALID_DATE_TEXTS)
        elif defect == "country_spelling":
            spelling = self.random.choice(record["country"].all_names())
            record["country_text"] = f" {self.random.choice((str.upper, str.lower, str.title))(spelling)} "
        elif defect == "bad_lifetime_value":
            record["lifetime_value"] = self.random.choice(("N/A", "", "unknown", "12.34.56"))
        elif defect == "negative_lifetime_value":
            record["lifetime_value"] = -max(1, record["lifetime_value"])

    def records(self, report: dict):
        """Endless stream of records for one source; the writer stops reading when its file is big enough."""
        defect_counts = report["defects"]
        while True:
            if self.customers_from_previous_sources and self.random.random() < self.overlap_ratio:
                customer = self.random.choice(self.customers_from_previous_sources)
                report["customers_also_in_earlier_sources"] += 1
            else:
                customer = self.new_customer()
                self.remember_for_overlap(customer)

            record = dict(customer)
            defect = self.random.choice(DEFECTS) if self.random.random() < self.dirty_ratio else None
            if defect:
                self.apply_defect(record, defect)
                defect_counts[defect] += 1
            yield record
            if defect == "duplicate_row":
                yield record

    # ---------- source formats ----------

    @staticmethod
    def crm_row(record: dict) -> list:
        return [
            record["customer_id"], record["first_name"], record["last_name"], record["email"], record["phone"],
            date_text(record["birth_date"], "%d.%m.%Y"),
            record["street"], record["city"], record["postal_code"],
            record["country_text"] or record["country"].name,
            record["registered_at"].strftime("%Y-%m-%d %H:%M:%S"),
            "Y" if record["is_active"] else "N",
            money_text(record["lifetime_value"], ","),
        ]

    @staticmethod
    def webshop_line(record: dict) -> str:
        lifetime_value = record["lifetime_value"]
        document = {
            "id": record["customer_id"],
            "firstName": record["first_name"],
            "lastName": record["last_name"],
            "email": record["email"],
            "phone": record["phone"],
            "birthDate": date_text(record["birth_date"], "%Y-%m-%d"),
            "address": {
                "street": record["street"],
                "city": record["city"],
                "zip": record["postal_code"],
                "countryCode": record["country_text"] or record["country"].code,
            },
            "createdAt": record["registered_at"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "active": record["is_active"],
            "ltv": lifetime_value if isinstance(lifetime_value, str) else lifetime_value / 100,
        }
        return json.dumps(document, ensure_ascii=False) + "\n"

    @staticmethod
    def legacy_item(record: dict) -> str:
        lines = (
            f"- member_id: {record['customer_id']}",
            f"  name: {yaml_quote(record['last_name'] + ', ' + record['first_name'])}",
            "  contact:",
            f"    email: {yaml_quote(record['email'])}",
            f"    phone: {yaml_quote(record['phone'])}",
            f"  born: {yaml_quote(date_text(record['birth_date'], '%Y/%m/%d'))}",
            "  location:",
            f"    street: {yaml_quote(record['street'])}",
            f"    city: {yaml_quote(record['city'])}",
            f"    postcode: {yaml_quote(record['postal_code'])}",
            f"    country: {yaml_quote(record['country_text'] or record['country'].native_name.lower())}",
            f"  joined: {yaml_quote(record['registered_at'].strftime('%Y-%m-%d %H:%M:%S'))}",
            f"  status: {yaml_quote('active' if record['is_active'] else 'inactive')}",
            f"  value: {yaml_quote(money_text(record['lifetime_value'], '.'))}",
        )
        return "\n".join(lines) + "\n"

    # ---------- files ----------

    def write_source(self, source: str, file_name: str, write_header, write_record):
        report = {"file": file_name, "records": 0, "customers_also_in_earlier_sources": 0, "defects": Counter()}
        self.current_source_sample = []
        self.current_source_new_customers = 0
        path = self.output_dir / file_name

        with path.open("w", encoding="utf-8", newline="") as handle:
            write_header(handle)
            for record_number, record in enumerate(self.records(report), start=1):
                write_record(handle, record, record_number)
                if record_number % SIZE_CHECK_INTERVAL == 0 and handle.tell() >= self.target_bytes:
                    report["records"] = record_number
                    break

        report["bytes"] = path.stat().st_size
        report["defects"] = dict(sorted(report["defects"].items()))
        self.source_reports[source] = report
        self.customers_from_previous_sources.extend(self.current_source_sample)
        print(f"  {file_name}: {report['records']:,} records, {report['bytes'] / 1024 / 1024:.1f} MB")

    def write_crm(self):
        writer = None

        def write_header(handle):
            nonlocal writer
            writer = csv.writer(handle, delimiter=";", lineterminator="\n")
            writer.writerow(("customer_id", "first_name", "last_name", "email", "phone", "birth_date",
                             "street", "city", "postal_code", "country", "registered_at", "active",
                             "lifetime_value"))

        def write_record(handle, record, record_number):
            writer.writerow(self.crm_row(record))

        self.write_source("crm", CRM_FILE, write_header, write_record)

    def write_webshop(self):
        self.write_source("webshop", WEBSHOP_FILE, lambda handle: None,
                          lambda handle, record, record_number: handle.write(self.webshop_line(record)))

    def write_legacy(self):
        def write_header(handle):
            handle.write(f"# Legacy member export, {self.yaml_batch_size} members per YAML document\n")

        def write_record(handle, record, record_number):
            if record_number % self.yaml_batch_size == 1 or self.yaml_batch_size == 1:
                handle.write("---\n")
            handle.write(self.legacy_item(record))

        self.write_source("legacy", LEGACY_FILE, write_header, write_record)

    def write_manifest(self):
        manifest = {
            "seed": self.seed,
            "target_megabytes": round(self.target_bytes / 1024 / 1024, 3),
            "dirty_ratio": self.dirty_ratio,
            "overlap_ratio": self.overlap_ratio,
            "unique_customers": self.next_customer_id - 1,
            "sources": self.source_reports,
        }
        (self.output_dir / MANIFEST_FILE).write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def zip_output(self) -> Path:
        zip_path = self.output_dir / "customers_data.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for file_name in (CRM_FILE, WEBSHOP_FILE, LEGACY_FILE, MANIFEST_FILE):
                archive.write(self.output_dir / file_name, arcname=file_name)
        print(f"  {zip_path.name}: {zip_path.stat().st_size / 1024 / 1024:.1f} MB")
        return zip_path

    def run(self, create_zip: bool = False):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Generating into {self.output_dir} (seed {self.seed}, ~{self.target_bytes / 1024 / 1024:.0f} MB per file)")
        self.write_crm()
        self.write_webshop()
        self.write_legacy()
        self.write_manifest()
        if create_zip:
            self.zip_output()
        print(f"Unique customers: {self.next_customer_id - 1:,}")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Generate CSV, JSON Lines and YAML customer exports.")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).with_name("data"))
    parser.add_argument("--size-mb", type=float, default=200, help="approximate size of each file")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dirty-ratio", type=float, default=0.02, help="share of records with a defect")
    parser.add_argument("--overlap-ratio", type=float, default=0.05,
                        help="share of records repeating a customer from an earlier source")
    parser.add_argument("--zip", action="store_true", help="also pack the files into customers_data.zip")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()
    DataGenerator(arguments.output_dir, arguments.size_mb, arguments.seed,
                  arguments.dirty_ratio, arguments.overlap_ratio).run(create_zip=arguments.zip)
