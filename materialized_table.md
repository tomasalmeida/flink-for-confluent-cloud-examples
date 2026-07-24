# Materialized table evolution example for `sample_data_orders`

This markdown is organized as a Cloud UI runbook.

## 0) Prerequisites

* Run each SQL block separately in a Flink SQL workspace.
* `sample_data_orders` is fed by the DataGen source connector for orders and is used as the source table in this example.
* `address` is a `ROW<city STRING, state STRING, zipcode BIGINT>`, so nested fields are accessed with dot notation like `address.city`.
* This example uses a materialized table so the pipeline can be evolved in place without creating a separate V2 output table.
* The evolution shown here is stateless and simply adds derived fields to the existing output shape.

## 1) Create V1 materialized table

```sql
CREATE MATERIALIZED TABLE orders_enriched_mt AS
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

## 2) Select from V1

```sql
SELECT *
FROM orders_enriched_mt;
```

## 3) Evolve the materialized table in place

This evolution keeps the same materialized table name and output topic, but updates the query to add new derived fields.

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

## 4) Select from the evolved materialized table

```sql
SELECT *
FROM orders_enriched_mt;
```

## 5) Inspect the current materialized table definition

```sql
SHOW CREATE MATERIALIZED TABLE orders_enriched_mt;
```

## Notes

* Unlike the carry-over v2 flow, this pattern evolves the same materialized table in place instead of creating `orders_enriched_v2` as a new table.
* `CREATE OR ALTER MATERIALIZED TABLE` keeps writing to the same output topic while updating the continuous query behind the materialized table.
* `START_MODE = RESUME_OR_FROM_BEGINNING` attempts to resume from the previous job position during evolution.
* Materialized-table evolutions are not idempotent. Running the same `CREATE OR ALTER MATERIALIZED TABLE` again still triggers a new evolution.
* If you later need to change only table properties, computed columns, metadata columns, or watermarks, use `ALTER MATERIALIZED TABLE` instead of a full query evolution.

## Reference docs

* [Materialized Tables concept](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html)
* [CREATE MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/create-materialized-table.html)
* [CREATE OR ALTER MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html)
* [ALTER MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-materialized-table.html)
* [SHOW CREATE MATERIALIZED TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/show.html#flink-sql-show-create-materialized-table)
* [Schema and Statement Evolution](https://docs.confluent.io/cloud/current/flink/concepts/schema-statement-evolution.html)
* [Flink SQL with options](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#flink-sql-with-options)

Short note on the configuration reference: the `Flink SQL with options` page is the canonical list of `WITH (...)` properties that can be applied to Flink tables and materialized tables, including Kafka topic settings, changelog behavior, serialization formats, Schema Registry settings, and startup or scan controls.
