# Carry-over v2 example for `sample_data_orders`

This markdown is organized as a Cloud UI runbook.

## 0) Prerequisites

* Run each SQL block separately in a Flink SQL workspace.
* `sample_data_orders` is fed by the DataGen source connector for orders and is used as the source table in this example.
* `address` is a `ROW<city STRING, state STRING, zipcode BIGINT>`, so nested fields are accessed with dot notation like `address.city`.
* The V1 and V2 statements are intentionally stateless so they fit the carry-over v2 model.
* Carry-over offsets references the old statement by statement name, so V1 is given an explicit name.

Reference docs: [Carry-over Offsets](https://docs.confluent.io/cloud/current/flink/operate-and-deploy/carry-over-offsets.html), [Flink SQL ROW type](https://docs.confluent.io/cloud/current/flink/reference/datatypes.html#flink-sql-row), [SET statement](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html)

## 1) V1 statement

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

## 2) Select from V1

```sql
SELECT *
FROM orders_enriched_v1;
```

## 3) Stop V1 before running V2

Before running V2, stop the statement named `orders-enriched-v1-statement` from the Statements view in Confluent Cloud UI.

If you run V2 while V1 is still running, V2 should remain pending until `orders-enriched-v1-statement` stops.

## 4) V2 statement using carry-over v2

```sql
SET 'client.statement-name' = 'orders-enriched-v2-statement';
SET 'sql.tables.initial-offset-from' = 'orders-enriched-v1-statement';

CREATE TABLE orders_enriched_v2 AS
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

## 5) Select from V2

```sql
SELECT *
FROM orders_enriched_v2;
```

## Notes

* `sql.tables.initial-offset-from` must reference the old statement name, not the table name. In this example, that is `orders-enriched-v1-statement`.
* In Cloud UI, `SET` must be submitted together with another statement, which is why the V1 and V2 blocks include both `SET` and `CREATE TABLE` together.
* This pattern is for stateless evolution. Avoid aggregates, windows, lag, pattern matching, and upsert-sink style migrations for carry-over v2.
