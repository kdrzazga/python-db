"""Reference data shared by the generator (value pools) and the ETL (country lookup).

Pools are deliberately small, so generated data is repetitive and compresses well.
Postal code patterns use '#' for a digit and '@' for an uppercase letter.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Country:
    code: str
    name: str
    native_name: str
    spelling_variants: tuple[str, ...]
    phone_prefix: str
    postal_pattern: str
    first_names: tuple[str, ...]
    last_names: tuple[str, ...]
    cities: tuple[str, ...]
    streets: tuple[str, ...]

    def all_names(self) -> tuple[str, ...]:
        return (self.code, self.name, self.native_name) + self.spelling_variants


COUNTRIES = (
    Country(
        code="PL", name="Poland", native_name="Polska", spelling_variants=("Rzeczpospolita Polska",),
        phone_prefix="48", postal_pattern="##-###",
        first_names=("Jan", "Anna", "Piotr", "Katarzyna", "Łukasz", "Małgorzata", "Tomasz", "Agnieszka", "Paweł", "Zofia"),
        last_names=("Nowak", "Kowalski", "Wiśniewski", "Wójcik", "Kamiński", "Lewandowski", "Zieliński", "Szymański"),
        cities=("Warszawa", "Kraków", "Wrocław", "Gdańsk", "Poznań", "Łódź", "Lublin"),
        streets=("Marszałkowska", "Długa", "Polna", "Lipowa", "Kościuszki", "Mickiewicza", "Słoneczna"),
    ),
    Country(
        code="DE", name="Germany", native_name="Deutschland", spelling_variants=("BRD", "Federal Republic of Germany"),
        phone_prefix="49", postal_pattern="#####",
        first_names=("Lukas", "Anna", "Maximilian", "Sophie", "Felix", "Marie", "Jonas", "Lena"),
        last_names=("Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker"),
        cities=("Berlin", "Hamburg", "München", "Köln", "Frankfurt", "Stuttgart"),
        streets=("Hauptstraße", "Schulstraße", "Gartenstraße", "Bahnhofstraße", "Dorfstraße", "Bergstraße"),
    ),
    Country(
        code="FR", name="France", native_name="France", spelling_variants=("République française",),
        phone_prefix="33", postal_pattern="#####",
        first_names=("Louis", "Camille", "Hugo", "Léa", "Jules", "Chloé", "Gabriel", "Manon"),
        last_names=("Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand"),
        cities=("Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes"),
        streets=("Rue de la Paix", "Rue Victor Hugo", "Avenue Jean Jaurès", "Rue Pasteur", "Rue de l'Église"),
    ),
    Country(
        code="ES", name="Spain", native_name="España", spelling_variants=("Espana", "Reino de España"),
        phone_prefix="34", postal_pattern="#####",
        first_names=("Hugo", "Lucía", "Martín", "Sofía", "Pablo", "María", "Alejandro", "Paula"),
        last_names=("García", "Fernández", "González", "Rodríguez", "López", "Martínez", "Sánchez", "Pérez"),
        cities=("Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza", "Málaga"),
        streets=("Calle Mayor", "Calle Real", "Avenida de la Constitución", "Calle del Sol", "Plaza España"),
    ),
    Country(
        code="IT", name="Italy", native_name="Italia", spelling_variants=("Repubblica Italiana",),
        phone_prefix="39", postal_pattern="#####",
        first_names=("Leonardo", "Sofia", "Francesco", "Giulia", "Alessandro", "Aurora", "Lorenzo", "Alice"),
        last_names=("Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo", "Ricci"),
        cities=("Roma", "Milano", "Napoli", "Torino", "Firenze", "Bologna"),
        streets=("Via Roma", "Via Garibaldi", "Via Mazzini", "Corso Italia", "Via Dante", "Via Verdi"),
    ),
    Country(
        code="GB", name="United Kingdom", native_name="United Kingdom", spelling_variants=("UK", "Great Britain", "England"),
        phone_prefix="44", postal_pattern="@@# #@@",
        first_names=("Oliver", "Olivia", "George", "Amelia", "Harry", "Isla", "Jack", "Emily"),
        last_names=("Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "Johnson", "Davies"),
        cities=("London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Bristol"),
        streets=("High Street", "Station Road", "Church Lane", "Victoria Road", "Park Avenue", "Mill Lane"),
    ),
    Country(
        code="US", name="United States", native_name="United States",
        spelling_variants=("USA", "U.S.A.", "United States of America", "America"),
        phone_prefix="1", postal_pattern="#####",
        first_names=("James", "Mary", "Michael", "Jennifer", "William", "Linda", "David", "Elizabeth"),
        last_names=("Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Wilson"),
        cities=("New York", "Chicago", "Houston", "Phoenix", "Seattle", "Boston"),
        streets=("Main Street", "Oak Street", "Maple Avenue", "Elm Street", "Washington Avenue", "Lake Drive"),
    ),
    Country(
        code="CZ", name="Czechia", native_name="Česko", spelling_variants=("Czech Republic", "Česká republika", "Cesko"),
        phone_prefix="420", postal_pattern="### ##",
        first_names=("Jakub", "Eliška", "Jan", "Tereza", "Tomáš", "Anna", "Matyáš", "Adéla"),
        last_names=("Novák", "Svoboda", "Novotný", "Dvořák", "Černý", "Procházka", "Kučera", "Veselý"),
        cities=("Praha", "Brno", "Ostrava", "Plzeň", "Liberec", "Olomouc"),
        streets=("Masarykova", "Nádražní", "Školní", "Husova", "Palackého", "Riegrova"),
    ),
)

EMAIL_DOMAINS = ("example.com", "mail.test", "inbox.example.org", "post.example.net")

# File names are shared by the generator (writes them) and the ETL (reads them).
CRM_FILE = "crm_customers.csv"
WEBSHOP_FILE = "webshop_customers.jsonl"
LEGACY_FILE = "legacy_members.yml"
MANIFEST_FILE = "manifest.json"
