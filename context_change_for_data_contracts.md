# Output data contract in another schema context for `sample_data_orders`

This markdown is organized as a Cloud UI runbook.

## 0) Prerequisites

* Run each SQL block separately in a Flink SQL workspace.
* `sample_data_orders` is fed by the DataGen source connector for orders and is used as the source table in this example.
* `address` is a `ROW<city STRING, state STRING, zipcode BIGINT>`, so nested fields are accessed with dot notation like `address.city`.
* This example creates a new derived table and stores the resulting output schema in a non-default Schema Registry context.
* Schema context names should start with a dot, for example `.orders-derived`.
* In CTAS, target table options must be provided in the `WITH (...)` clause.

Reference docs:

* [CREATE TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html)
* [ALTER TABLE schema context](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html)
* [Schema contexts](https://docs.confluent.io/cloud/current/sr/schemas-manage.html)
* [All table configuration options](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#flink-sql-with-options)

The All table configuration options page is the canonical reference for everything you can put inside the `WITH (...)` clause of `CREATE TABLE` and CTAS statements. It is the right place to check supported properties for changelog behavior, connector settings, Kafka topic retention and cleanup, key/value serialization, Schema Registry context options, and scan startup or bounded-read settings.

## 1) Create a derived table whose output data contract is stored in another context

```sql
SET 'client.statement-name' = 'orders-context-statement';

CREATE TABLE orders_enriched_other_context
WITH (
  'value.format' = 'avro-registry',
  'value.avro-registry.schema-context' = '.orders-derived'
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
  address.zipcode AS zipcode,
  CONCAT(address.city, ', ', address.state) AS city_state,
  orderunits * 2.0 AS doubled_units
FROM sample_data_orders
WHERE orderunits IS NOT NULL;
```

## 2) Query the result table

```sql
SELECT *
FROM orders_enriched_other_context;
```

## 3) Optional verification

If you want to confirm the table property inside Flink SQL, run:

```sql
SHOW CREATE TABLE orders_enriched_other_context;
```

You should see the target table configured with:

```sql
'value.avro-registry.schema-context' = '.orders-derived'
```

## Notes

* This stores the output value schema for `orders_enriched_other_context` in the `.orders-derived` Schema Registry context instead of the default context.
* If you want a different context, replace `.orders-derived` with your own context name.
* If you also need the key schema in a non-default context, define an explicit key format and add `key.avro-registry.schema-context` as well.
* Contexts are logical sub-registries in Schema Registry, so the same subject name can exist independently in different contexts.
