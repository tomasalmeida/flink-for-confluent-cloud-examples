# Adopt a table fed by two `INSERT INTO` statements as a materialized table (`UNION ALL`)

This markdown is organized as a Cloud UI runbook.

This runbook is a hands-on variant of [`ctas_to_materialized_table_adopt_existing_topic.md`](./ctas_to_materialized_table_adopt_existing_topic.md). Instead of one CTAS statement, the existing table is fed by **two separate `INSERT INTO` statements**, one per input topic. You stop both, then adopt the table in place as a single materialized table whose query is a `UNION ALL` of the two inputs.

Scenario:

1. Two input topics (`orders_a`, `orders_b`), each with 10 events produced manually.
2. A target table (`orders_all`, the third topic).
3. Two `INSERT INTO orders_all` statements, one per input topic.
4. Stop both inserts and capture their source offsets.
5. Adopt `orders_all` as a materialized table using `UNION ALL`, starting at the captured offsets.
6. Produce 2 more events into each input topic.
7. Read the materialized table and confirm all 24 events are present exactly once.

> **Important:** Adoption does not take over the running `INSERT INTO` statements. Stop both first. If either keeps running, two jobs write to the same topic. The materialized table query starts with empty state and does not resume the old statements' positions, so you supply the start offsets yourself (step 4).

> **Verify against the current docs.** Confirm syntax and adoption rules in [Adopt an existing table](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html). Run this in a disposable environment first.

## 0) Prerequisites and safety rules

* Run each SQL block separately in a Flink SQL workspace.
* Every table is created with `DISTRIBUTED INTO 1 BUCKETS`, so each topic has a single partition and offsets are easy to read.
* **Adoption is one-way.** A materialized table cannot be converted back to a plain table.
* Do not run `DROP TABLE orders_all`. It deletes the backing topic and its records.
* Use a disposable environment. Names below are examples.

## 1) Create the two input topics and produce 10 events in each

Creating a Flink table also creates its backing Kafka topic and Schema Registry subject.

```sql
CREATE TABLE orders_a (
  id INT,
  item STRING,
  amount DOUBLE
) DISTRIBUTED INTO 1 BUCKETS;

CREATE TABLE orders_b (
  id INT,
  item STRING,
  amount DOUBLE
) DISTRIBUTED INTO 1 BUCKETS;
```

Produce 10 events manually into each topic. Use distinct id ranges so you can tell the sources apart: 1 to 10 for `orders_a` and 101 to 110 for `orders_b`.

```sql
INSERT INTO orders_a VALUES
  (1,  'apple',  1.0),
  (2,  'banana', 2.0),
  (3,  'cherry', 3.0),
  (4,  'date',   4.0),
  (5,  'elder',  5.0),
  (6,  'fig',    6.0),
  (7,  'grape',  7.0),
  (8,  'honey',  8.0),
  (9,  'iris',   9.0),
  (10, 'jam',   10.0);
```

```sql
INSERT INTO orders_b VALUES
  (101, 'kiwi',   11.0),
  (102, 'lemon',  12.0),
  (103, 'mango',  13.0),
  (104, 'nectar', 14.0),
  (105, 'olive',  15.0),
  (106, 'peach',  16.0),
  (107, 'quince', 17.0),
  (108, 'rhubarb',18.0),
  (109, 'sage',   19.0),
  (110, 'tomato', 20.0);
```

Each `VALUES` insert is bounded and finishes on its own. Check the contents:

```sql
SELECT COUNT(*) AS n FROM orders_a;
SELECT COUNT(*) AS n FROM orders_b;
```

Both should return 10. Cancel each read after validation.

## 2) Create the target table (third topic)

The target adds an `origin` column so you can see which input each row came from.

```sql
CREATE TABLE orders_all (
  id INT,
  item STRING,
  amount DOUBLE,
  origin STRING
) DISTRIBUTED INTO 1 BUCKETS;
```

## 3) Start the two `INSERT INTO` statements

Give each statement a fixed name. You need the names to describe them in step 5.

```sql
SET 'client.statement-name' = 'orders-all-insert-a';

INSERT INTO orders_all
SELECT id, item, amount, 'A' AS origin
FROM orders_a;
```

```sql
SET 'client.statement-name' = 'orders-all-insert-b';

INSERT INTO orders_all
SELECT id, item, amount, 'B' AS origin
FROM orders_b;
```

Both statements are unbounded and keep running. Wait until each has processed its 10 events:

```sql
SELECT origin, COUNT(*) AS n FROM orders_all GROUP BY origin;
```

Expect `A = 10` and `B = 10`. Cancel the read after validation. Do not continue until both counts are reached, because stopping early would capture offsets before all events are processed.

## 4) Stop both inserts

Use either 4.a (command line) or 4.b (Cloud UI). Either way, this is the migration boundary, and it ensures no old job writes to `orders_all` once it is adopted.

### 4.a) Via the command line

```bash
confluent flink region use --cloud aws --region us-east-1

confluent flink statement stop orders-all-insert-a
confluent flink statement stop orders-all-insert-b
```

Confirm both are `STOPPED`:

```bash
confluent flink statement describe orders-all-insert-a -o json
confluent flink statement describe orders-all-insert-b -o json
```

### 4.b) Via the UI

1. In the Cloud UI, open **Flink** and select your environment and compute pool.
2. Open the **Statements** tab and find `orders-all-insert-a`.
3. Open the statement's actions menu (or its details page) and choose **Stop statement**. Confirm.
4. Repeat for `orders-all-insert-b`.
5. Refresh the list and confirm both statuses are `STOPPED`.

### After stopping

While stopped, nothing is added to `orders_all`. Keep this window short.

## 5) Capture the source offsets of each statement

Use either 5.a (command line) or 5.b (Cloud UI). Both give the same values.

### 5.a) Via the command line

```bash
confluent flink region use --cloud aws --region us-east-1

confluent flink statement describe orders-all-insert-a -o json
confluent flink statement describe orders-all-insert-b -o json
```

Copy each statement's source entry from `latest_offsets`. Expected shape:

```json
{
  "status": "STOPPED",
  "latest_offsets": {
    "orders_a": "partition:0,offset:10"
  }
}
```

```json
{
  "status": "STOPPED",
  "latest_offsets": {
    "orders_b": "partition:0,offset:10"
  }
}
```

Use the complete values returned, including every partition. Do not copy offsets from the output topic `orders_all`.

### 5.b) Via the UI

1. In the Cloud UI, open **Flink** and select your environment and compute pool.
2. Open the **Statements** tab and find `orders-all-insert-a`.
3. Click the statement to open its details. Confirm the status is `STOPPED`.
4. In the details panel, find the source offsets (the `latest_offsets` entry for `orders_a`) and copy the full value, for example `partition:0,offset:10`.
5. Go back to the Statements list, open `orders-all-insert-b`, and copy the value for `orders_b` the same way.

If the offsets are not visible in the UI for your version, use 5.a.

Use the complete values shown, including every partition. Do not use offsets from the output topic `orders_all`.

### Capture the schema

Also capture the existing schema, which the adopting statement must match:

```sql
SHOW CREATE TABLE orders_all;
```

## 6) Adopt `orders_all` as a materialized table with `UNION ALL`

Run `CREATE OR ALTER MATERIALIZED TABLE` against the **existing** table name. Because the table already exists, Flink adopts it in place and keeps its topic and data.

Each `UNION ALL` branch starts at the offsets captured from its own stopped statement. In this example each input topic has one partition and exactly 10 events, so both offsets are known: `partition:0,offset:10`. They are set below. If your `describe` output differs, use your values instead, along with your own `SHOW CREATE TABLE` schema.

```sql
CREATE OR ALTER MATERIALIZED TABLE orders_all (
  id INT,
  item STRING,
  amount DOUBLE,
  origin STRING
)
AS
SELECT id, item, amount, 'A' AS origin
FROM orders_a
  /*+ OPTIONS(
    'scan.startup.mode' = 'specific-offsets',
    'scan.startup.specific-offsets' = 'partition:0,offset:10'
  ) */
UNION ALL
SELECT id, item, amount, 'B' AS origin
FROM orders_b
  /*+ OPTIONS(
    'scan.startup.mode' = 'specific-offsets',
    'scan.startup.specific-offsets' = 'partition:0,offset:10'
  ) */;
```

Do not add a `START_MODE` clause. The source hints supply the start positions.

Why not the other start modes:

* `START_MODE = FROM_BEGINNING` replays all 20 existing events and appends them, leaving duplicates (ids 1 to 10 and 101 to 110 twice).
* `START_MODE = FROM_NOW` can miss events produced between the stop and the start.

Support for source hints inside a materialized table statement can depend on environment and version. If the statement is rejected, do not drop anything. Either accept a bounded duplicate window with `FROM_BEGINNING` and deduplicate on `id`, or use the side-by-side approach in [`ctas_to_materialized_table_migration.md`](./ctas_to_materialized_table_migration.md).

Confirm in the Materialized Tables view that `orders_all` shows as a materialized table and is running. Confirm the two stopped `INSERT INTO` statements stay stopped.

## 7) Produce 2 more events into each input topic

```sql
INSERT INTO orders_a VALUES
  (11, 'kale',   21.0),
  (12, 'lime',   22.0);
```

```sql
INSERT INTO orders_b VALUES
  (111, 'melon', 23.0),
  (112, 'nut',   24.0);
```

## 8) Show the result on the materialized table

```sql
SELECT * FROM orders_all ORDER BY id;
```

Expected: 24 rows, with no duplicates.

| origin | ids |
|---|---|
| A | 1 to 12 |
| B | 101 to 112 |

Check counts and duplicates:

```sql
SELECT origin, COUNT(*) AS n, COUNT(DISTINCT id) AS distinct_ids
FROM orders_all
GROUP BY origin;
```

Expect `A: 12 / 12` and `B: 12 / 12`. Equal `n` and `distinct_ids` means no replayed records.

Check the boundary specifically. Ids 10 and 110 (the last events before the stop) must appear exactly once, and ids 11, 12, 111, 112 (the first events after adoption) must be present:

```sql
SELECT id, COUNT(*) AS n
FROM orders_all
WHERE id IN (10, 11, 110, 111)
GROUP BY id;
```

Each id should have `n = 1`. If ids 10 or 110 show `n = 2`, or ids 11 or 111 are missing, the offset convention differs from what you pasted. Check whether the captured offset is the last processed or the next offset, and correct it in a disposable run.

## 9) What this shows

* The topic, name, and first 20 records of `orders_all` are unchanged by adoption.
* One materialized table now replaces two `INSERT INTO` statements. New events from both inputs flow through it.
* Starting each branch at its captured offsets avoids both gaps and duplicates.

## Rollback

* **Before step 6:** restart `orders-all-insert-a` and `orders-all-insert-b` per the normal statement procedure. Nothing has changed. Restarting them resumes from their own checkpointed positions.
* **After step 6:** adoption is one-way. Fix the query with another `CREATE OR ALTER MATERIALIZED TABLE` on the same name, checking the start mode first because evolution discards processing state. Do not restart the old inserts alongside it.

## Reference docs

* [Materialized Tables concept](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html)
* [CREATE OR ALTER MATERIALIZED TABLE (Adopt an existing table)](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html)
* [INSERT INTO](https://docs.confluent.io/cloud/current/flink/reference/statements/insert-into-from-select.html)
* [confluent flink statement describe](https://docs.confluent.io/confluent-cli/current/command-reference/flink/statement/confluent_flink_statement_describe.html)
* [Schema and Statement Evolution](https://docs.confluent.io/cloud/current/flink/concepts/schema-statement-evolution.html)
