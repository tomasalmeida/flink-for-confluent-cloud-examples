# Migrate a `CREATE TABLE AS` pipeline to a materialized table without deleting data

This markdown is organized as a Cloud UI runbook.

This runbook migrates the `CREATE TABLE AS SELECT` (CTAS) pipeline from [`carry_over_v2_evolution.md`](./carry_over_v2_evolution.md) to a materialized table **without dropping the existing table or its backing topic**.

The migration is intentionally side-by-side:

* The existing CTAS table, `orders_enriched_v1`, remains available as the rollback path.
* The new materialized table uses a different catalog name, `orders_enriched_mt`, and therefore a different backing Kafka topic.
* Consumers are moved only after the new pipeline has been validated.
* The old table is stopped after cutover but is not deleted.

> **Important:** Confluent Cloud currently supports materialized tables only as new tables. It does not provide a non-destructive conversion of an existing CTAS table, reuse of its backing topic, or topic renaming. Keeping the old name would require a drop-and-recreate operation, which is destructive. If preserving data is the priority, do not attempt a same-name migration.

## 0) Prerequisites and safety rules

* Run each SQL block separately in a Flink SQL workspace.
* `sample_data_orders` is fed by the DataGen source connector for orders and is the source table in this example.
* `address` is a `ROW<city STRING, state STRING, zipcode BIGINT>`, so nested fields are accessed with dot notation such as `address.city`.
* This example assumes a stateless projection. Aggregates, windows, joins with state, lag, pattern matching, and upsert-sink pipelines need additional validation because a new materialized table rebuilds processing state from its configured start position.
* Confirm that the source retains enough history for the new materialized table to process the required data. The recommended safe default below is `START_MODE = FROM_BEGINNING`.
* Inventory downstream consumers before starting the migration. Use Stream Lineage and the Kafka topic details in the Cloud UI to identify Flink statements, connectors, applications, and other readers.
* Reserve a new name that will remain stable after cutover. In this runbook that name is `orders_enriched_mt`.
* Do not run `DROP TABLE orders_enriched_v1` or `DROP MATERIALIZED TABLE orders_enriched_mt` as part of this migration.
* Keep the old CTAS pipeline running until the new materialized table has passed validation and the consumer cutover is complete.

### What this runbook protects

The old backing topic and its records remain intact throughout the migration. If the new materialized table fails validation, leave consumers on `orders_enriched_v1` and fix or remove only the new materialized table after confirming that it contains no data that must be retained.

### What this runbook does not preserve automatically

The new materialized table has a new topic. Kafka consumer offsets are topic-specific, so external consumers cannot continue from the old topic's offsets automatically. Plan a consumer cutover, dual-read period, or application-level deduplication strategy as appropriate for the consumer.

## 1) Baseline: the existing CTAS pipeline

This is the V1 statement being migrated. It creates `orders_enriched_v1` and continuously populates it. The migration intentionally stops this statement at a controlled boundary before creating the new materialized table.

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

If this pipeline is already running, keep it running while completing the baseline checks in step 2. Stop it only in step 3, after the definition and validation sample have been captured.

## 2) Capture the definition and confirm the old pipeline is healthy

Keep the output of this statement. Reuse the `SELECT` definition in step 4.

```sql
SHOW CREATE TABLE orders_enriched_v1;
```

Confirm that the old pipeline is producing data before stopping it:

```sql
SELECT * FROM orders_enriched_v1;
```

Rows should appear and continue updating while the query runs. Cancel the bounded read after validation. If no rows arrive, resolve the source or statement-health issue before continuing.

Also record:

* The existing statement name: `orders-enriched-v1-statement`.
* The old output topic and Schema Registry subject, from the Cloud UI or Stream Lineage.
* The downstream consumers that must move to the new output.
* A validation sample, such as recent `orderid` values and their expected enriched fields.

## 3) Stop the existing statement and capture its source offsets

Stop `orders-enriched-v1-statement` from the Statements view. This creates the migration boundary: the new materialized table will start from the source position reached by this stopped statement.

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

Use the complete value returned by the command, including every source partition. Do not copy offsets from the old output topic.

The old CTAS table and its backing topic are still present and retain their records. From this point until the new materialized table is healthy, consumers can remain on the old topic, but new source records will not be added to the old output while the statement is stopped.

## 4) Create the materialized table from the stopped statement boundary

Create the new materialized table under the reserved name `orders_enriched_mt`. Do not reuse `orders_enriched_v1`.

The source scan hint below uses the offsets captured from the stopped statement. This is the preferred path when the migration must continue from that exact statement boundary rather than replaying all retained source history.

Before production use, validate this statement in a disposable workspace or non-production environment because support for dynamic source options inside `CREATE MATERIALIZED TABLE ... AS SELECT` can be environment/version dependent. If the statement is rejected, do not change or drop the old table; use the `FROM_BEGINNING` fallback in the next section.

```sql
CREATE MATERIALIZED TABLE orders_enriched_mt
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

Do not add a second startup mode to this statement. The source hint supplies the starting offsets.

### Fallback when exact-offset startup is unavailable

If the target environment does not accept the source hint for a materialized table, leave the old table intact and recreate only the new table with a full retained-history replay:

```sql
CREATE MATERIALIZED TABLE orders_enriched_mt
START_MODE = FROM_BEGINNING
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
WHERE orderunits IS NOT NULL;
```

`FROM_BEGINNING` may replay records already processed by the old statement, but it does not skip retained history. Do not use `START_MODE = FROM_NOW` for a lossless replacement because it excludes records produced after the old statement stopped and before the new table was created.

## 5) Wait for the new pipeline to catch up

Monitor `orders_enriched_mt` in the Materialized Tables or Statements view.

Wait until:

* The materialized table is healthy and running.
* Its source-processing lag or Messages Behind metric has reached the expected steady state.
* New source records continue to appear in the new output.
* The output contains the records at and after the captured source boundary.

Run a bounded read against the new table and compare it with the validation sample from step 2:

```sql
SELECT * FROM orders_enriched_mt;
```

For this stateless projection, verify that records have the same filtering and derived-field behavior:

* `orderunits IS NOT NULL` is applied consistently.
* `city`, `state`, and `zipcode` match the nested `address` fields.
* Recent `orderid` values appear in the expected output.
* No unexpected schema, serialization, key, or partitioning differences were introduced.

If you used the exact-offset path, pay special attention to the boundary. Confirm the first records after the stopped statement's source offsets are present and that the offset convention has not introduced a duplicate or gap. Make downstream processing idempotent during validation.

## 5) Validate downstream compatibility before cutover

Before moving consumers, validate the new output topic and schema with the same access patterns used by production consumers.

Check:

* Consumer principals have permission to read the new topic and schema subject.
* Kafka key and value formats are compatible with the consumer.
* The output schema is compatible with the consumer's deserializer.
* Connectors, Flink statements, dashboards, and applications have been identified in Stream Lineage.
* Any consumer that relies on the old topic name has an explicit configuration change prepared.

Because the topic is new, do not assume that a consumer group can simply resume from its old offset. Choose one of these cutover patterns:

* **Consumer pause and switch:** pause the consumer, switch it to the new topic, then resume from a deliberately selected position.
* **Dual-read and deduplicate:** read both topics during the transition and deduplicate using a stable business key such as `orderid`.
* **Application-specific handoff:** use the consumer's documented checkpoint or replay mechanism to establish the new starting position.

For an append-only downstream consumer, a dual-read or replay-and-deduplicate period is usually safer than silently starting the new topic at its latest offset.

## 6) Cut consumers over to the new materialized table

Move each downstream consumer from `orders_enriched_v1` to `orders_enriched_mt` using the cutover plan from step 5.

For downstream Flink SQL, update the source table reference to `orders_enriched_mt` and deploy the change according to the downstream statement's own migration procedure. For external Kafka consumers or connectors, update the configured topic and validate the consumer's new records.

After the cutover, verify:

* Consumers are reading from the new topic.
* Consumer lag is healthy.
* Records continue arriving with the expected enriched shape.
* No consumer is still unintentionally dependent on the old topic.
* The old CTAS output remains available and unchanged as the rollback path.

## 7) Confirm the old CTAS pipeline remains stopped—without deleting its output

The old statement was intentionally stopped in step 3 before the new materialized table was created. Confirm that it remains `STOPPED` during validation and consumer cutover:

```text
Statement: orders-enriched-v1-statement
Expected status: STOPPED
```

Stopping the statement pauses production but preserves the CTAS table, its backing Kafka topic, and its records. Do **not** run:

```sql
DROP TABLE orders_enriched_v1;
```

That command is not cleanup-only: it deletes the old table's backing topic and records. Retain the old table until your retention, audit, and rollback policy allows its eventual retirement. If it must be retired later, handle that as a separate, explicitly approved data-retention change.

## 8) Rollback procedure

### Before the cutover

Consumers remain on `orders_enriched_v1` while the new materialized table is validated. If `orders_enriched_mt` fails or produces incorrect results, leave consumers on the old topic and investigate the new table without deleting the old table.

### After the cutover

If rollback is required, redeploy or resume `orders-enriched-v1-statement` according to the normal statement procedure. Allow it to catch up from its last committed source position, then switch consumers back to the old topic using the rollback configuration.

The old topic and its prior records remain available because this runbook stops—but never drops—the old CTAS table.

## 9) Evolve the materialized table in place from now on

Once the new pipeline is established, use `CREATE OR ALTER MATERIALIZED TABLE` for future query changes. This keeps the materialized table name and its backing topic stable while Flink manages the in-place evolution.

For this stateless projection:

```sql
CREATE OR ALTER MATERIALIZED TABLE orders_enriched_mt
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
* `RESUME_OR_FROM_BEGINNING` attempts to resume an existing materialized table from its prior position; on first creation it falls back to processing from the beginning.
* Materialized-table evolution is not idempotent. Re-running the same command triggers another evolution.
* Processing state is discarded during evolution. Stateful queries may recalculate results and can produce duplicates, stale records, or missing retractions depending on changelog mode.
* Use `ALTER MATERIALIZED TABLE` for property-only changes, computed columns, metadata columns, or watermarks when a full query evolution is not required.

Inspect the current definition at any time:

```sql
SHOW CREATE MATERIALIZED TABLE orders_enriched_mt;
```

## Notes on the rejected same-name approach

The original workflow stopped the CTAS statement, captured source offsets, dropped `orders_enriched_v1`, and recreated a materialized table with the same name. That approach is not non-destructive:

* A materialized table must be created as a new table and owns a new backing topic.
* The existing CTAS table cannot be converted in place or attached to its existing topic.
* `DROP TABLE orders_enriched_v1` deletes the old backing topic and its records.
* Reusing the name does not preserve the topic identity or consumer offsets.
* When continuing from the stopped statement is required, capture its `latest_offsets` and use the source `specific-offsets` hint shown in step 4 only after validating that the target environment accepts it. If it is unavailable, use the non-destructive `FROM_BEGINNING` fallback.

If retaining the exact catalog name or topic is a hard requirement, document the trade-off explicitly and obtain approval for the destructive drop-and-recreate. It is not compatible with the no-deletion objective of this runbook.

## Reference docs

* [Materialized Tables concept](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html)
* [CREATE MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/create-materialized-table.html)
* [CREATE OR ALTER MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html)
* [ALTER MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-materialized-table.html)
* [DROP TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/drop-table.html)
* [confluent flink statement describe](https://docs.confluent.io/confluent-cli/current/command-reference/flink/statement/confluent_flink_statement_describe.html)
* [Schema and Statement Evolution](https://docs.confluent.io/cloud/current/flink/concepts/schema-statement-evolution.html)
* [Stream Lineage](https://docs.confluent.io/cloud/current/stream-governance/stream-lineage.html)
