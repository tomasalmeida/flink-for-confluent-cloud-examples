# Migrate a `CREATE TABLE AS` pipeline to a materialized table in place (adopt the existing table)

This markdown is organized as a Cloud UI runbook.

This runbook migrates the `CREATE TABLE AS SELECT` (CTAS) pipeline from [`carry_over_v2_evolution.md`](./carry_over_v2_evolution.md) to a materialized table by **adopting the existing table and its backing Kafka topic in place**. It supersedes the side-by-side approach in [`ctas_to_materialized_table_migration.md`](./ctas_to_materialized_table_migration.md) when you want to keep the table name, topic, data, and consumers.

Materialized tables in Confluent Cloud for Apache Flink can now run on a Kafka topic that already exists. You point a `CREATE OR ALTER MATERIALIZED TABLE` statement at an existing table, and Flink adopts it as a materialized table in place. The topic, its data, and the consumers reading it are kept. Previously, a materialized table always created its own new topic.

What changes compared with the side-by-side runbook:

| | Side-by-side runbook | This runbook (adopt in place) |
|---|---|---|
| Table name | New (`orders_enriched_mt`) | Unchanged (`orders_enriched_v1`) |
| Backing topic | New topic | Existing topic, kept |
| Consumer cutover | Required, with offset handling | Not required |
| Old data | Stays on old topic only | Stays in the same topic |
| Rollback | Switch consumers back | Limited, see step 8 |

> **Important:** Adoption does not take over the CTAS statement that is already writing to the topic. Stop that statement first. If both run, two jobs write to the same topic. The new materialized table query also starts with empty state and does not resume the old statement's position, so the start position is the main correctness decision (step 4).

> **Verify against the current docs.** Confirm the exact syntax and adoption rules in [Adopt an existing table](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html) before production use. Validate the full runbook in a disposable environment first.

## 0) Prerequisites and safety rules

* Run each SQL block separately in a Flink SQL workspace.
* `sample_data_orders` is fed by the DataGen source connector for orders and is the source table in this example.
* `address` is a `ROW<city STRING, state STRING, zipcode BIGINT>`, so nested fields are accessed with dot notation such as `address.city`.
* This example assumes a stateless projection. Aggregates, windows, stateful joins, lag, pattern matching, and upsert-sink pipelines need additional validation because the adopted materialized table starts with empty processing state.
* **Adoption is one-way.** Once the table is a materialized table, you cannot convert it back to a plain CTAS table. Treat it as a committed change.
* Back up what you cannot recreate. The topic and data are kept, but the old statement's progress is not. Capture the stopped statement's offsets (step 3) and a validation sample (step 2).
* Confirm that the source retains enough history from the captured offsets.
* Inventory downstream consumers with Stream Lineage and the Kafka topic details. They do not need to change, but you should know who to monitor.
* Do not run `DROP TABLE orders_enriched_v1`. It deletes the backing topic and its records, which is the opposite of the goal here.

### What this runbook protects

The topic, its records, the schema subject, and consumer group offsets all stay in place. Consumers keep reading the same topic by the same name throughout.

### What this runbook does not preserve automatically

* **The old statement's position.** The materialized table does not resume from it. You supply the start position yourself.
* **Processing state.** It starts empty. This is harmless for a stateless projection.
* **A clean way back to CTAS.** See step 8.

## 1) Baseline: the existing CTAS pipeline

This is the V1 statement being migrated. It creates `orders_enriched_v1` and continuously populates it.

```sql
SET 'client.statement-name' = 'orders-enriched-v1-statement';

CREATE TABLE orders_enriched_v1 AS
SELECT
  `key`,
  ordertime,
  orderid,
  itemid,
  orderunits,
  address,
  address.city AS city,
  address.state AS state,
  address.zipcode AS zipcode
FROM sample_data_orders
WHERE orderunits IS NOT NULL;
```

Keep it running while completing step 2. Stop it only in step 3.

## 2) Capture the definition and confirm the old pipeline is healthy

Capture the exact schema of the existing table. The adopting statement must declare a schema that matches it, and you will paste this output into step 4.

```sql
SHOW CREATE TABLE orders_enriched_v1;
```

Keep the output. Pay attention to column names, types, nullability, key columns, the distribution/bucketing clause, and table options. Flink rejects the conversion when the declared schema does not match, and a matching schema keeps the schema already registered in Schema Registry unchanged.

Confirm that the old pipeline is producing data before stopping it:

```sql
SELECT * FROM orders_enriched_v1;
```

Rows should appear and continue updating while the query runs. Cancel the read after validation. If no rows arrive, resolve the source or statement-health issue before continuing.

Also record:

* The existing statement name: `orders-enriched-v1-statement`.
* The output topic and Schema Registry subject, from the Cloud UI or Stream Lineage. These will not change.
* The downstream consumers and their current lag.
* A validation sample, such as recent `orderid` values and their expected enriched fields, and the current record count or latest offsets of the output topic.

## 3) Stop the existing statement and capture its source offsets

Stop `orders-enriched-v1-statement` from the Statements view. This creates the migration boundary: the adopting statement will start from the source position reached by this stopped statement. Stopping first also ensures only one job writes to the topic.

Confirm that the statement status is `STOPPED`, then describe it with the CLI:

```bash
confluent flink region use --cloud aws --region us-east-1

confluent flink statement describe orders-enriched-v1-statement -o json
```

Copy the `sample_data_orders` value from the `latest_offsets` map. For example:

```json
{
  "status": "STOPPED",
  "latest_offsets": {
    "sample_data_orders": "partition:0,offset:100;partition:1,offset:79;partition:2,offset:84"
  }
}
```

Use the complete value returned by the command, including every source partition. Do not copy offsets from the output topic.

While the statement is stopped, no new records are added to the output topic. Consumers stay connected and simply see no new data until step 5 completes. Keep this window short.

## 4) Adopt the existing table as a materialized table

Run `CREATE OR ALTER MATERIALIZED TABLE` against the **existing** table name. Because a table with that name already exists, Flink adopts it in place instead of creating a new topic.

### Choosing the start position

The adopted query starts with empty state and does not resume the old job's position, so choose the start position deliberately. Both of the common modes are risky here, because the data is written into the same topic that already holds the old records:

* `START_MODE = FROM_NOW` can **miss rows** produced between the old statement stopping and the new one starting.
* `START_MODE = FROM_BEGINNING` **reprocesses all retained source history** and appends it to the existing topic, which duplicates records that are already there.

For this reason, the preferred path is to start exactly where the old statement stopped, using the source offsets captured in step 3.

### Preferred: start from the captured offsets

Paste the schema from `SHOW CREATE TABLE` in step 2 into the column list. The columns below are illustrative. Use your captured output, including key and distribution clauses.

```sql
CREATE OR ALTER MATERIALIZED TABLE orders_enriched_v1 (
  `key` BYTES,
  ordertime BIGINT,
  orderid INT,
  itemid STRING,
  orderunits DOUBLE,
  address ROW<city STRING, state STRING, zipcode BIGINT>,
  city STRING,
  state STRING,
  zipcode BIGINT
)
AS
SELECT
  `key`,
  ordertime,
  orderid,
  itemid,
  orderunits,
  address,
  address.city AS city,
  address.state AS state,
  address.zipcode AS zipcode
FROM sample_data_orders
  /*+ OPTIONS(
    'scan.startup.mode' = 'specific-offsets',
    'scan.startup.specific-offsets' = '<paste the complete sample_data_orders latest_offsets value here>'
  ) */
WHERE orderunits IS NOT NULL;
```

Do not add a `START_MODE` clause to this statement. The source hint supplies the starting offsets.

Support for dynamic source options inside a materialized table statement can be environment and version dependent, so validate this in a disposable workspace first. If the statement is rejected, do not drop anything. Choose one of the fallbacks below.

### Fallbacks when exact-offset startup is unavailable

* **Accept a bounded duplicate window.** Use `START_MODE = FROM_BEGINNING` and make downstream consumers idempotent, deduplicating on `orderid`. Only do this when all consumers can tolerate replayed records, and expect the topic to contain the replayed history in addition to the old records.
* **Accept a bounded gap.** Use `START_MODE = FROM_NOW` only if losing the records produced while the old statement was stopped is acceptable.
* **Use the side-by-side runbook.** If neither is acceptable, use [`ctas_to_materialized_table_migration.md`](./ctas_to_materialized_table_migration.md), which writes to a new topic and avoids mixing replayed records with existing ones.

## 5) Wait for the adopted pipeline to catch up

Monitor `orders_enriched_v1` in the Materialized Tables or Statements view. It should now show as a materialized table.

Wait until:

* The materialized table is healthy and running.
* Its source-processing lag or Messages Behind metric has reached the expected steady state.
* New source records continue to appear in the output topic.

Run a bounded read and compare it with the validation sample from step 2:

```sql
SELECT * FROM orders_enriched_v1;
```

Verify:

* `orderunits IS NOT NULL` is applied consistently.
* `city`, `state`, and `zipcode` match the nested `address` fields.
* Recent `orderid` values appear in the expected output.
* The schema subject is unchanged and no new schema version was unexpectedly registered.
* The topic name, partition count, and key format are unchanged.

If you used the exact-offset path, pay special attention to the boundary. Confirm that the first records after the stopped statement's source offsets are present and that the offset convention has not introduced a duplicate or gap.

## 6) Validate consumers

No cutover is needed, because the topic and name are unchanged. Still confirm that consumers are unaffected:

* Consumer groups resumed reading and their lag returns to normal.
* No deserialization errors, which would point to a schema mismatch.
* Downstream Flink statements, connectors, and dashboards report healthy status.
* Records continue arriving with the expected enriched shape.

Consumers that tolerate duplicates are unaffected if you used a `FROM_BEGINNING` fallback. Consumers that do not should be checked for replayed records.

## 7) Confirm the old statement is gone from the write path

The old CTAS statement was stopped in step 3 and must not be restarted. The materialized table now owns writes to the topic. Restarting `orders-enriched-v1-statement` would create a second writer on the same topic.

Do not delete the stopped statement's history until the migration is accepted, since it holds the offsets you captured. Do **not** run:

```sql
DROP TABLE orders_enriched_v1;
```

That deletes the table's backing topic and its records.

## 8) Rollback procedure

Adoption is one-way, so rollback is different from the side-by-side approach.

### Before step 4

Restart `orders-enriched-v1-statement` according to the normal statement procedure. Nothing has changed.

### After step 4

You cannot convert the materialized table back to a plain CTAS table. If the adopted pipeline is wrong:

* Fix the query with another `CREATE OR ALTER MATERIALIZED TABLE` against the same name. Evolution is not idempotent and discards processing state, so check the start mode first.
* Do not restart the old CTAS statement alongside the materialized table.
* If the output topic was corrupted, for example by a `FROM_BEGINNING` replay, recover it from a copy you took before step 4 or by a planned reprocessing. This is the main cost of adopting in place, and it is the reason to prefer the side-by-side runbook when you need a clean way back.

## 9) Evolve the materialized table in place from now on

Use `CREATE OR ALTER MATERIALIZED TABLE` for future query changes. This keeps the name and backing topic stable while Flink manages the in-place evolution.

```sql
CREATE OR ALTER MATERIALIZED TABLE orders_enriched_v1
START_MODE = RESUME_OR_FROM_BEGINNING
AS
SELECT
  `key`,
  ordertime,
  orderid,
  itemid,
  orderunits,
  address,
  address.city AS city,
  address.state AS state,
  address.zipcode AS zipcode,
  orderunits * 2.0 AS doubled_units,
  CONCAT(address.city, ', ', address.state) AS city_state
FROM sample_data_orders
WHERE orderunits IS NOT NULL;
```

Important evolution behavior:

* `CREATE OR ALTER MATERIALIZED TABLE` performs an in-place migration and writes to the same output topic.
* `RESUME_OR_FROM_BEGINNING` resumes an existing materialized table from its prior position. It falls back to the beginning only when there is no prior position.
* Evolution is not idempotent. Re-running the same command triggers another evolution.
* Processing state is discarded during evolution. Stateful queries may recalculate results and can produce duplicates, stale records, or missing retractions depending on changelog mode.
* Use `ALTER MATERIALIZED TABLE` for property-only changes, computed columns, metadata columns, or watermarks when a full query evolution is not required.

Inspect the current definition at any time:

```sql
SHOW CREATE MATERIALIZED TABLE orders_enriched_v1;
```

## Reference docs

* [Materialized Tables concept](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html)
* [CREATE OR ALTER MATERIALIZED TABLE (Adopt an existing table)](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html)
* [CREATE MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/create-materialized-table.html)
* [ALTER MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-materialized-table.html)
* [DROP TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/drop-table.html)
* [confluent flink statement describe](https://docs.confluent.io/confluent-cli/current/command-reference/flink/statement/confluent_flink_statement_describe.html)
* [Schema and Statement Evolution](https://docs.confluent.io/cloud/current/flink/concepts/schema-statement-evolution.html)
* [Stream Lineage](https://docs.confluent.io/cloud/current/stream-governance/stream-lineage.html)
