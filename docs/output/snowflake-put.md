# Snowflake PUT Output

Writes batched messages to a Snowflake stage with optional Snowpipe ingestion.

## Why `snowflake_put` over `sql_insert`

`sql_insert` issues row-by-row `INSERT`s that run on a Snowflake virtual warehouse, so all the ingestion cost lands on warehouse compute (billed per second). `snowflake_put` stages batched files and, when a Snowpipe is configured (key-pair auth plus a pre-created pipe), loads them through Snowpipe, Snowflake's serverless ingestion path billed per GB, so the warehouse no longer runs the ingestion (a warehouse must still be configured). For continuous, high-volume loads this is much cheaper. The benefit assumes files are batched to a reasonable size (Snowflake recommends ~100-250 MB); many tiny files add queue overhead that cancels out the saving. Without a Snowpipe configured, `snowflake_put` only stages files and a warehouse-run `COPY INTO` is still needed to load them. See Snowflake's [Snowpipe billing](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-billing) docs for current rates.

## Source

Ported from the upstream [warpstreamlabs/bento](https://github.com/warpstreamlabs/bento) project (MIT license).

## Documentation

For configuration, examples, and behavior, refer to the upstream bento documentation:

- **[warpstreamlabs/bento — `snowflake_put` docs](https://warpstreamlabs.github.io/bento/docs/components/outputs/snowflake_put/)**

## Quick example

```yaml
output:
  snowflake_put:
    account: my_account
    user: my_user
    private_key_file: /path/to/rsa_key.pem
    role: ACCOUNTADMIN
    database: MY_DB
    warehouse: COMPUTE_WH
    schema: PUBLIC
    stage: "@%MY_TBL"
    path: ingest
    compression: AUTO
    snowpipe: MY_PIPE
    batching:
      count: 100
      period: 5s
      processors:
        - archive:
            format: concatenate
```

## Notes

- Snowpipe requires key-pair authentication (`private_key_file`); user/password auth only supports PUT to stages, not Snowpipe.
- Encrypted private keys must use PKCS#5 v2.0 (e.g. `openssl pkcs8 -topk8 -v2 des3`). PKCS#5 v1.5 is not supported.
- The underlying [gosnowflake](https://github.com/snowflakedb/gosnowflake) driver needs write access to the OS temp directory.
