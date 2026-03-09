"""
Customer data generation for synthetic test fixtures.

Provides two related generators:

1. forge_customers: Generates a table of unique customer profiles. Each customer
   is deterministically derived from its CUST-XXXXXX identifier via SHA-256, so
   the same customer_id always produces the same profile fields regardless of when
   or how many times it is called.

2. forge_customer_sample_assignments: Assigns sample barcodes to customers,
   creating the many-to-one relationship between samples and customers. Includes
   submission-level fields (crop_type, sample_date) that are independently maskable.

The helper derive_seed_from_barcode is exposed as a public utility for use in
dbt unit test fixture generation — run it in Python to get deterministic input
values for YAML fixture rows.
"""

import hashlib

import numpy as np
import pandas as pd


FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer",
    "Michael", "Linda", "David", "Barbara", "William", "Susan",
    "Richard", "Jessica", "Joseph", "Sarah", "Thomas", "Karen",
    "Charles", "Lisa", "Christopher", "Nancy", "Daniel", "Betty",
    "Matthew", "Margaret", "Anthony", "Sandra", "Mark", "Ashley",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
    "Jackson", "Martin",
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
    "icloud.com", "protonmail.com", "aol.com", "live.com",
    "me.com", "mail.com", "comcast.net", "att.net",
]

STREET_NAMES = [
    "Old Mill", "Creek", "Valley", "Ridge", "Hollow",
    "Meadow", "Orchard", "Harvest", "River Bend", "Farmhouse",
    "Prairie", "Windmill", "Pasture", "County Road", "Township",
    "Rural Route", "Grain Mill", "Silo", "Cornfield", "Fence Post",
]

STREET_SUFFIXES = ["Rd", "Dr", "Trl", "Pike", "Loop", "Hwy"]

CITIES = [
    "Springfield", "Franklin", "Clinton", "Georgetown", "Salem",
    "Riverside", "Madison", "Oakland", "Burlington", "Fairview",
    "Greenville", "Bristol", "Marion", "Clayton", "Monroe",
    "Oxford", "Arlington", "Lexington", "Princeton", "Dover",
]

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

NOTES_PHRASES = [
    "Rush processing requested",
    "Sample collected pre-irrigation",
    "Post-harvest soil assessment",
    "Spring planting preparation",
    "Repeat from same field section",
    "Field recently treated with lime",
    "Irrigation water source changed",
    "New crop rotation beginning",
    "Organic certification required",
    "Suspected micronutrient deficiency",
    "Baseline sample for new acreage",
    "Follow-up to prior season results",
    "Drip irrigation system installed",
    "Cover crop incorporated last month",
    "Comparison plot for trial program",
]

CROP_TYPES = [
    "Corn", "Soybeans", "Wheat", "Tomatoes", "Almonds",
    "Cotton", "Grapes", "Blueberries", "Strawberries", "Rice",
]


def derive_seed_from_barcode(barcode: str) -> int:
    """
    Derive a deterministic numpy random seed from any string identifier.

    Uses SHA-256 to hash the identifier and extracts the first 8 bytes as an
    unsigned 64-bit integer. The same input always produces the same seed,
    guaranteeing reproducible fixture generation without storing any state.

    Works on any string — barcodes, customer IDs, or any other identifier —
    making it a general-purpose building block for deterministic generation.

    Args:
        barcode: Any string identifier to derive a seed from.

    Returns:
        A non-negative integer suitable for seeding numpy.random.default_rng.

    Example:
        >>> seed1 = derive_seed_from_barcode("PYB1234-5678")
        >>> seed2 = derive_seed_from_barcode("PYB1234-5678")
        >>> seed1 == seed2
        True
    """
    digest = hashlib.sha256(barcode.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def _forge_street_address(generator) -> str:
    """
    Generate a realistic rural street address.

    About 20% of generated addresses use the Wisconsin PLSS coordinate format
    (e.g., N1254W2398), common in the rural Midwest and occasionally found
    verbatim in vendor lab reports — exactly the kind of thing that breaks a
    naive address parser.

    Args:
        generator: numpy random Generator.

    Returns:
        A street address string.
    """
    if generator.random() < 0.20:
        # Public Land Survey System (PLSS) coordinate address, common in Wisconsin
        # and parts of the Midwest. Format: {N|S}{4-digit}{E|W}{4-digit}
        north_south_dir = generator.choice(["N", "S"])
        east_west_dir = generator.choice(["E", "W"])
        north_south_coord = int(generator.integers(1000, 9999))
        east_west_coord = int(generator.integers(1000, 9999))
        return f"{north_south_dir}{north_south_coord}{east_west_dir}{east_west_coord}"

    street_number = int(generator.integers(100, 9999))
    street_name = STREET_NAMES[generator.integers(0, len(STREET_NAMES))]
    street_suffix = STREET_SUFFIXES[generator.integers(0, len(STREET_SUFFIXES))]
    return f"{street_number} {street_name} {street_suffix}"


def _forge_customer_profile(customer_id: str) -> dict:
    """
    Generate a complete customer profile deterministically from a customer_id.

    All fields are derived from a single SHA-256-based seed, so the same
    customer_id always produces the same profile. This is the internal
    implementation — callers should use forge_customers for batch generation.

    Args:
        customer_id: The CUST-XXXXXX identifier to derive the profile from.

    Returns:
        A dict with all customer profile fields.
    """
    seed = derive_seed_from_barcode(customer_id)
    profile_generator = np.random.default_rng(seed)

    first_name = FIRST_NAMES[profile_generator.integers(0, len(FIRST_NAMES))]
    last_name = LAST_NAMES[profile_generator.integers(0, len(LAST_NAMES))]

    age = int(profile_generator.integers(25, 76))  # 76 exclusive → range is 25–75
    birth_year = 2026 - age
    birth_month = int(profile_generator.integers(1, 13))
    birth_day = int(profile_generator.integers(1, 29))  # Cap at 28 to avoid invalid Feb dates
    date_of_birth = f"{birth_year:04d}-{birth_month:02d}-{birth_day:02d}"

    email_domain = EMAIL_DOMAINS[profile_generator.integers(0, len(EMAIL_DOMAINS))]
    email = f"{first_name.lower()}.{last_name.lower()}@{email_domain}"

    area_code = int(profile_generator.integers(200, 999))
    exchange_code = int(profile_generator.integers(200, 999))
    line_number = int(profile_generator.integers(1000, 9999))
    phone = f"({area_code}) {exchange_code}-{line_number}"

    street_address = _forge_street_address(profile_generator)

    city = CITIES[profile_generator.integers(0, len(CITIES))]
    state = STATES[profile_generator.integers(0, len(STATES))]
    zip_code = f"{int(profile_generator.integers(10000, 99999)):05d}"

    notes = NOTES_PHRASES[profile_generator.integers(0, len(NOTES_PHRASES))]

    return {
        "customer_id": customer_id,
        "customer_name": f"{first_name} {last_name}",
        "date_of_birth": date_of_birth,
        "age": age,
        "email": email,
        "phone": phone,
        "street_address": street_address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "notes": notes,
    }


def forge_customers(n_customers: int, generator) -> pd.DataFrame:
    """
    Generate a DataFrame of unique synthetic customer profiles.

    Each customer receives a unique CUST-XXXXXX identifier, and all profile
    fields are derived deterministically from that identifier via SHA-256.
    Given the same generator seed, this function always produces the same
    set of customers.

    Args:
        n_customers: Number of unique customer records to generate.
        generator: numpy random Generator used to produce unique customer IDs.
                   Use np.random.default_rng(42) for reproducible output.

    Returns:
        pd.DataFrame with columns: customer_id, customer_name, date_of_birth,
        age, email, phone, street_address, city, state, zip_code, notes.

    Example:
        >>> gen = np.random.default_rng(42)
        >>> df = forge_customers(5, gen)
        >>> len(df)
        5
        >>> df["customer_id"].nunique() == 5
        True
    """
    seen_ids: set[str] = set()
    records = []
    while len(records) < n_customers:
        customer_id_num = int(generator.integers(0, 16**6))
        customer_id = f"CUST-{customer_id_num:06X}"
        if customer_id in seen_ids:
            continue
        seen_ids.add(customer_id)
        records.append(_forge_customer_profile(customer_id))

    return pd.DataFrame(records)


def forge_customer_sample_assignments(
    barcodes: list[str],
    customers_df: pd.DataFrame,
    generator,
) -> pd.DataFrame:
    """
    Assign each sample barcode to a customer and generate submission-level fields.

    Multiple barcodes can map to the same customer — this models the real-world
    pattern where a farm submits many samples across different fields or seasons.
    Each assignment also includes crop_type and sample_date, which are
    commercially sensitive and can be masked independently of customer identity.

    Args:
        barcodes: List of sample barcode strings to assign.
        customers_df: DataFrame produced by forge_customers. Must have a
                      customer_id column and at least one row.
        generator: numpy random Generator for assignment and field generation.

    Returns:
        pd.DataFrame with columns: barcode, customer_id, crop_type, sample_date.
        One row per barcode.

    Raises:
        ValueError: If customers_df is empty.

    Example:
        >>> gen = np.random.default_rng(42)
        >>> customers = forge_customers(3, gen)
        >>> barcodes = ["PYB001", "PYB002", "PYB003"]
        >>> assignments = forge_customer_sample_assignments(barcodes, customers, gen)
        >>> len(assignments)
        3
        >>> set(assignments["customer_id"]).issubset(set(customers["customer_id"]))
        True
    """
    if len(customers_df) == 0:
        raise ValueError(
            "customers_df cannot be empty — cannot assign barcodes to zero customers."
        )

    if len(barcodes) == 0:
        return pd.DataFrame(columns=["barcode", "customer_id", "crop_type", "sample_date"])

    n_barcodes = len(barcodes)
    customer_indices = generator.integers(0, len(customers_df), size=n_barcodes)
    customer_ids = customers_df["customer_id"].iloc[customer_indices].values

    crop_indices = generator.integers(0, len(CROP_TYPES), size=n_barcodes)
    crop_types = [CROP_TYPES[idx] for idx in crop_indices]

    start_date = np.datetime64("2026-01-01")
    day_offsets = generator.integers(0, 365, size=n_barcodes)
    sample_dates = np.datetime_as_string(
        start_date + day_offsets.astype("timedelta64[D]"), unit="D"
    )

    return pd.DataFrame({
        "barcode": barcodes,
        "customer_id": customer_ids,
        "crop_type": crop_types,
        "sample_date": sample_dates,
    })
