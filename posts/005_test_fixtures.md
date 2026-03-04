# Synthetic Data and the Privacy Problem: Building a Customer Pipeline Without Production Records

The fixtures in this series have always been honest about what they were optimizing for. Posts 1 through 3 generated vendor CSV files designed to capture structural chaos: typos in column headers, shifting measurement packages, metadata rows that Spark reads with misplaced confidence. The goal was a bronze layer that could absorb whatever shape a vendor file arrived in without requiring code changes. The fixture data itself, a collection of pH readings and copper concentrations, was never the point. Nobody's privacy interests are implicated by a synthetic soil sample.

Customer records are a different matter entirely.

A soil lab does not only process measurements. It processes submissions from real farms, research institutions, and agricultural businesses. Names, addresses, billing contacts, field histories. The moment a pipeline needs to model that relationship, "generate some plausible-looking data" stops being a casual decision and starts being a question worth taking seriously. What does realistic mean for sensitive data? How do you test against customer records when the real ones are legally and ethically off-limits? And if you cannot use real data, how do you know your fixtures are testing the right things?

This post answers those questions by building two tools that address related but distinct problems. The customer generator produces realistic profiles derived deterministically from barcodes that already exist in the pipeline. The masking library addresses a separate situation entirely: when real production data needs to enter a development environment with appropriate controls applied. Both tools are useful. They are not interchangeable.

The code for this post can be found [here](). Feel free to follow along or dig in if you want more details.

## What the Customer Generator Produces

The names, addresses, and contact details come from numpy's random number generator seeded with a SHA-256 digest of each barcode. The same barcode produces the same customer profile every time, in every environment, without storing anything. Faker would have produced more linguistically convincing output and probably should have been the tool for this job. The fixtures work fine regardless. Realism of content was never the point. Stability of attachment was.

The mechanism is SHA-256 used as a pure function into numpy's seed space:

```python
def derive_seed_from_barcode(barcode: str) -> int:
    digest = hashlib.sha256(barcode.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")
```

Given any string, this returns a deterministic non-negative integer suitable for seeding `numpy.random.default_rng`. The same string always produces the same seed, which means the same `customer_id` always produces the same profile, regardless of when or how many times the generator runs. This is referential stability without a database.

The customer data divides into two tables. A `customers` table holds one row per unique customer: name, address, contact info, date of birth, age, and a free-text notes field that sounds like it came from someone who has been doing soil testing long enough to have opinions about irrigation timing. A `customer_samples` table holds one row per barcode, linking it to a customer with submission-level fields: `crop_type` and `sample_date`. The separation matters because customers are entities and samples are facts. A corn farmer submits many soil samples across many fields and seasons. Flattening that into a single table would denormalize the relationship in ways that create trouble the moment you want customer-level aggregation or need to update a profile field.

```python
customers_df = forge_customers(len(all_barcodes) // 5, gen)
customer_samples_df = forge_customer_sample_assignments(all_barcodes, customers_df, gen)
```

The five-to-one ratio means most customers appear across multiple sample submissions, which reflects how actual lab relationships work. One detail worth mentioning: the address generator includes a Wisconsin PLSS coordinate format with about 20% probability. `N5024W3295` is a real addressing convention from the Public Land Survey System, common in rural parts of the Midwest. It shows up verbatim in actual vendor lab reports (usually to the quiet dismay of whoever first encounters it), which means it shows up in the fixtures, which means any address parsing logic gets tested against it. That is the point of realistic fixtures.

## Why Not Just Use the Generated Data Directly?

That raises a legitimate question, though. Randomly generated data is structurally valid but statistically hollow. The real distributions, the genuine quirks, the specific shenanigans your data gets up to when nobody is watching: all of that is gone, replaced by Alice and Bob. Those two are perfectly serviceable for unit testing logic in isolation. Based on the number of unit test failures I've seen with them, I would not trust them together at a bar on a Saturday night. Similarly, I would not trust them as a proxy for production data's full range of quirks. If the goal is integration testing, fixtures that never existed in the real world can only tell you so much.

This is where masking enters as a separate tool for a separate problem.

## The Masking Strategies

The alternative to generating synthetic data is masking real data. Synthetic generation works when you need data that never existed and want full control over its structure. Masking works when you need a development dataset derived from real production records, preserving real distributions, real anomalies, and real edge cases that a generator might miss. In practice, many teams need both: generated data for early development, masked production data for integration testing before launch.

Four strategies cover the meaningful design space, although this is not meant to exhaustive, merely illustrative.

**Shuffle** permutes values within a column independently, which preserves the marginal distribution: if 30% of customers are in Wisconsin before shuffling, 30% still are afterward. Every value in the column is real. Every value passed format validation before masking and will pass it again after. Nothing looks wrong.

The hazard is relational, not statistical. Shuffle severs the connection between a value and the row it belonged to. Sometimes that severance is the goal. The association between a particular farm and a particular set of samples can be as sensitive as the customer record itself. For end-to-end testing, whether barcode LAB-001 actually belongs to Sandra Hernandez is irrelevant to whether the pipeline processes it correctly, and severing that link adds a layer of protection if the development environment is ever compromised. Shuffle breaks the relationship deliberately and completely, which is occasionally exactly what a privacy requirement demands.

Where shuffle becomes hazardous is when the relationship itself is load-bearing. A dataset released for external research might need customer histories intact but otherwise anonymized to be analytically meaningful. Shuffle would destroy that meaning while preserving the appearance of validity: joins complete, results look plausible, and the underlying analysis is quietly wrong. That use case is outside the scope of what we are building here, but it is worth understanding before reaching for shuffle by default.

The strategy is not wrong. It requires knowing whether the relationship you are severing was one you needed to keep. Most teams discover this distinction at an inopportune time.

**Imputation** replaces column values with output from a callable. The masking module takes an `imputers` dictionary mapping column names to functions that accept a row count and return a list of replacement values:

```python
imputers = {"customer_name": lambda n: ["Anonymous"] * n}
masked = mask_impute(customers_df, ["customer_name"], imputers)
```

Every customer becomes Anonymous. No distribution is preserved, no format inference required. For fields where the actual value is irrelevant to what you are testing, that is perfectly sufficient.

The limitation is the flip side of the explicitness. A dataset where every customer name is Anonymous is not anonymized in any meaningful sense; it is a dataset with a broken name column. If downstream logic does anything with name format, uniqueness, or even basic non-nullness, imputation will expose that dependency immediately. This is occasionally useful information. A pipeline that silently assumes customer names are unique has a latent bug that imputation will surface faster than any other strategy.

The callable interface also means imputation can do more than substitute a constant. A more sophisticated imputer could draw from a list of plausible replacements, apply format rules, or generate values that satisfy downstream validation constraints. Anonymous is the simplest possible implementation. The mechanism supports considerably more nuance when the situation calls for it.

**Resampling** handles numeric columns by fitting a normal distribution to the existing values and drawing a fresh set of values clipped to the original range:

```python
column_mean = original_values.mean()
column_std = original_values.std()
resampled = generator.normal(column_mean, column_std, size=n)
masked_df[column] = np.clip(resampled, original_values.min(), original_values.max())
```

The statistical shape of the column is preserved: similar mean, similar spread, values that fall within the original bounds. A data scientist working with a resampled age column will see a realistic distribution without seeing any real ages. For downstream logic that cares about aggregates rather than individual values, this is the most analytically faithful masking strategy available.

**Hashing** applies SHA-256 and keeps the first eight hex characters:

```python
masked_df[column] = masked_df[column].apply(
    lambda value: hashlib.sha256((salt + str(value)).encode()).hexdigest()[:8]
)
```

The critical property is determinism. The same input with the same salt always produces the same output, which means hashed values can be joined across tables. Hash `customer_id` in both `customers` and `customer_samples` using the same salt and `CUST-CE8B39` becomes `c2fd6c19` in both places. The foreign key relationship survives. This is the only masking strategy where that is true.

The tradeoffs are real and worth understanding. An eight-character hex string truncated from SHA-256 is not cryptographically robust, particularly for low-cardinality columns where brute force recovery is straightforward. A salt raises the cost of that attack without eliminating it. Hash masking is appropriate for development environments where the goal is protecting data from casual exposure, not for datasets approaching public distribution.

The salt also creates an operational dependency worth naming explicitly. Lose the salt, and your masked datasets become unrelatable across tables and unreproducible from scratch. It belongs in environment configuration alongside your database credentials, not in the codebase.

## Combining the Strategies

The four strategies combine into a single `apply_masking` call that dispatches per column based on a configuration dictionary:

```python
mask_config = {
    "customer_id": "hash",
    "customer_name": "impute",
    "date_of_birth": "shuffle",
    "email": "shuffle",
    "phone": "shuffle",
    "street_address": "shuffle",
    "city": "shuffle",
    "age": "resample",
}
masked_customers = apply_masking(
    customers_df,
    mask_config,
    generator=gen,
    imputers={"customer_name": lambda n: ["Anonymous"] * n},
    salt=MASK_SALT,
)
```

The `customer_samples` table requires the same salt on `customer_id` to preserve the join:

```python
masked_assignments = apply_masking(
    customer_samples_df,
    {"customer_id": "hash", "crop_type": "shuffle"},
    generator=gen,
    salt=MASK_SALT,
)
```

The fact that this requires explicit decisions about every column is a feature. It forces the question of what each field actually is before deciding how to treat it. That is a conversation worth having before the data enters a development environment, not after.

## What the Masked Data Actually Looks Like

We can now compare `customers.csv` and `customers_masked.csv`. One row from each:

**Original:**
```
CUST-CE8B39, Sandra Hernandez, 1975-09-01, 51, sandra.hernandez@protonmail.com,
(923) 867-3934, 4332 Hollow Trl, Oxford, WV, 27369, Comparison plot for trial program
```

**Masked:**
```
c2fd6c19, Anonymous, 1957-07-21, 72.89, mark.jones@aol.com,
(364) 820-3448, 4848 Pasture Dr, Dover, WV, 27369, Comparison plot for trial program
```

Most of this looks reasonable until you look carefully. The customer ID is hashed, the name is imputed, the date of birth, phone, street address, and city have been shuffled to values that belonged to different customers.

The age has been resampled to 72.89. The masked output is technically within range and statistically plausible in aggregate, but no human being has ever reported themselves as 72.89 years old. Any schema that enforces INTEGER on that column will reject it immediately. This is the kind of thing that looks fine in a test that checks whether a value exists and looks obviously wrong the moment anyone actually reads it.

Then there is the email address.

`mark.jones@aol.com` is sitting in the masked output fully readable. Shuffling an email address does not protect it. It reassigns it. Sandra's email is now attached to someone else's row and Mark's is attached to Sandra's. Both are still completely exposed. If the goal was protecting contact information, the masking configuration failed quietly and completely.

The city and zip code tell a similar story. Oxford shuffled to Dover, but the zip code stayed as 27369. Column-level masking applied independently has no awareness of relationships between columns. The result passes any single-column validation and fails the moment anything checks whether the address makes geographic sense.

None of these are bugs in the masking implementation (OK, the age thing is). All of them are the correct output of the configuration we provided. That is precisely the point: the strategies do exactly what they are told, not what you meant.

## The Silver Layer Extension and Unit Tests

The fixture infrastructure is only useful if something actually runs against it. Before loading masked data into a test schema and invoking `dbt build`, it helps to know that the model logic itself is correct. The unit test handles that first.

`int_lab_samples_with_customers` performs a two-hop join across three models:

```sql
FROM int_lab_samples_standardized
LEFT JOIN stg_customer_samples
    ON int_lab_samples_standardized.sample_barcode = stg_customer_samples.sample_barcode
LEFT JOIN stg_customers
    ON stg_customer_samples.customer_id = stg_customers.customer_id
```

The model adds `has_customer_assignment`, a boolean that is `TRUE` when a measurement's barcode has a matching row in `customer_samples`, which makes unmatched measurements findable without requiring every downstream query to filter on NULL customer fields.

The join has three distinct behavioral regimes worth testing explicitly: a barcode with a full match through both hops, a barcode with no customer assignment at all, and a barcode that matches `customer_samples` but whose `customer_id` has no corresponding row in `customers`. That third case, an orphaned assignment, is realistic enough to justify its own fixture row. Anyone who has spent meaningful time with production data has encountered some version of this: a submission that arrived before its parent record, or a customer that got cleaned up while their samples stayed behind. A barcode submitted before a customer record existed, or after one was deleted, should produce `has_customer_assignment: true` and `customer_name: null`. Testing it explicitly verifies the full join chain rather than just the happy path.

Once the unit tests pass, the masked fixtures are ready to do their actual job. Load `masked_customers` and `masked_assignments` into a test schema, point `dbt build --select your_models --target test` at it, and you have an integration test that exercises the pipeline against data with the structural properties and relational complexity of production records, without any of the liability.


## Closing

The masking strategies covered here represent a design space, not a checklist. Shuffle preserves distributions but severs relationships, deliberately or otherwise. Imputation is explicit but analytically hollow. Resampling maintains statistical shape but loses type fidelity. Hashing preserves referential integrity at the cost of cryptographic robustness. No single strategy is correct in isolation. The configuration you choose reflects a set of tradeoffs that are worth making consciously rather than discovering later when something downstream produces confident, well-formatted, wrong answers.

The fixture infrastructure this post builds serves a specific purpose: giving the pipeline realistic data to run against without the liability of real records. The masked customer dataset is not a privacy guarantee. It is a development tool, and like any development tool its value depends on using it for the right job. Getting the configuration right before you need it is substantially easier than explaining a masking decision you made at 4pm on a Thursday.

---

**Complete working example:** The labforge customer and masking modules are in [src/labforge/](../src/labforge/). The dbt models are in [src/crucible/models/silver/](../src/crucible/models/silver/). Example data comparing raw and masked output is in [example_data/](../example_data/).