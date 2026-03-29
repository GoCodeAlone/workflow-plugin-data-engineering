# workflow-plugin-data-engineering

Data engineering plugin for the [GoCodeAlone/workflow](https://github.com/GoCodeAlone/workflow) engine. Provides CDC, lakehouse (Iceberg), time-series (InfluxDB, TimescaleDB, ClickHouse, QuestDB, Druid), graph (Neo4j), data quality, schema migrations, multi-tenancy, and data catalog (DataHub, OpenMetadata) capabilities.

## Install

Download from [Releases](https://github.com/GoCodeAlone/workflow-plugin-data-engineering/releases) or install via wfctl:

```bash
wfctl plugin install data-engineering
```

Place the binary + `plugin.json` in your workflow server's plugins directory.

## Module Types

### CDC

#### `cdc.source`

Change Data Capture from PostgreSQL, MySQL, or DynamoDB via Bento, Debezium, or AWS DMS.

```yaml
modules:
  - name: aurora_cdc
    type: cdc.source
    config:
      provider: bento          # bento | debezium | dms
      source_id: aurora-users
      source_type: postgres
      connection: "${ config(\"aurora_dsn\") }"
      tables:
        - public.users
        - public.orders
      options:
        slot_name: workflow_cdc
        publication: workflow_pub
```

| Provider | Backend | Use Case |
|----------|---------|----------|
| `bento` | Bento/RedPanda Connect (postgres_cdc, mysql binlog, dynamodb_streams) | Default. Single binary, no Kafka required |
| `debezium` | Kafka Connect REST API | Enterprise. Requires Kafka Connect cluster |
| `dms` | AWS DMS via SDK v2 | AWS-native. Aurora/RDS to Kinesis/Kafka |

#### `data.tenancy`

Multi-tenant data isolation with three strategies.

```yaml
modules:
  - name: tenancy
    type: data.tenancy
    config:
      strategy: schema_per_tenant  # schema_per_tenant | db_per_tenant | row_level
      tenant_key: ctx.tenant_id
      schema_prefix: "t_"
      # db_per_tenant: connection_template: "postgres://localhost/tenant_{{tenant}}"
      # row_level: tenant_column: org_id, tables: [users, orders]
```

### Lakehouse

#### `catalog.iceberg`

Apache Iceberg REST Catalog connection.

```yaml
modules:
  - name: analytics_catalog
    type: catalog.iceberg
    config:
      endpoint: "${ config(\"iceberg_catalog_url\") }"
      warehouse: "s3://data-lake/warehouse"
      credential: "${ config(\"iceberg_token\") }"
      httpTimeout: 30s
```

#### `lakehouse.table`

Iceberg table lifecycle management.

```yaml
modules:
  - name: users_table
    type: lakehouse.table
    config:
      catalog: analytics_catalog
      namespace: [analytics, raw]
      table: users
      schema:
        fields:
          - { name: id, type: long, required: true }
          - { name: email, type: string, required: true }
          - { name: created_at, type: timestamptz }
```

### Time-Series

All time-series modules implement a shared `TimeSeriesWriter` interface (WritePoint, WriteBatch, Query), so the common steps (`step.ts_write`, `step.ts_query`, etc.) work with any backend.

#### `timeseries.influxdb`

```yaml
modules:
  - name: metrics
    type: timeseries.influxdb
    config:
      url: "${ config(\"influx_url\") }"
      token: "${ config(\"influx_token\") }"
      org: my-org
      bucket: default
      batchSize: 1000
      flushInterval: 1s
      precision: ns
```

#### `timeseries.timescaledb`

```yaml
modules:
  - name: tsdb
    type: timeseries.timescaledb
    config:
      connection: "${ config(\"timescale_dsn\") }"
      maxOpenConns: 25
      hypertables:
        - { table: metrics, timeColumn: time, chunkInterval: 1d }
```

#### `timeseries.clickhouse`

```yaml
modules:
  - name: analytics_ch
    type: timeseries.clickhouse
    config:
      endpoints: ["clickhouse1:9000", "clickhouse2:9000"]
      database: analytics
      username: "${ config(\"ch_user\") }"
      password: "${ config(\"ch_pass\") }"
      compression: lz4
      secure: true
```

#### `timeseries.questdb`

```yaml
modules:
  - name: questdb
    type: timeseries.questdb
    config:
      ilpEndpoint: "localhost:9009"
      httpEndpoint: "http://localhost:9000"
      authToken: "${ config(\"questdb_token\") }"
```

#### `timeseries.druid`

```yaml
modules:
  - name: druid
    type: timeseries.druid
    config:
      routerUrl: "http://druid-router:8888"
      username: "${ config(\"druid_user\") }"
      password: "${ config(\"druid_pass\") }"
      httpTimeout: 60s
```

### Graph

#### `graph.neo4j`

```yaml
modules:
  - name: knowledge_graph
    type: graph.neo4j
    config:
      uri: "bolt://neo4j:7687"
      database: neo4j
      username: "${ config(\"neo4j_user\") }"
      password: "${ config(\"neo4j_pass\") }"
      maxConnectionPoolSize: 50
```

### Data Catalog

#### `catalog.schema_registry`

Confluent Schema Registry.

```yaml
modules:
  - name: schema_reg
    type: catalog.schema_registry
    config:
      endpoint: "http://schema-registry:8081"
      username: "${ config(\"sr_user\") }"
      password: "${ config(\"sr_pass\") }"
      defaultCompatibility: BACKWARD
```

#### `catalog.datahub`

```yaml
modules:
  - name: catalog
    type: catalog.datahub
    config:
      endpoint: "http://datahub-gms:8080"
      token: "${ config(\"datahub_token\") }"
```

#### `catalog.openmetadata`

```yaml
modules:
  - name: catalog
    type: catalog.openmetadata
    config:
      endpoint: "http://openmetadata:8585"
      token: "${ config(\"om_token\") }"
```

### Data Quality

#### `quality.checks`

```yaml
modules:
  - name: data_quality
    type: quality.checks
    config:
      provider: builtin     # builtin | dbt | soda | great_expectations
      contractsDir: ./contracts/
      database: my-database
```

### Migrations

#### `migrate.schema`

```yaml
modules:
  - name: app_migrations
    type: migrate.schema
    config:
      strategy: both          # declarative | scripted | both
      target: my-database
      schemas:
        - path: schemas/users.yaml
      migrationsDir: ./migrations/
      lockTable: schema_migrations
      onBreakingChange: block  # block | warn | blue_green
```

## Step Types

### CDC Steps

| Step | Config | Output |
|------|--------|--------|
| `step.cdc_start` | `source_id` | `{action, source_id}` |
| `step.cdc_stop` | `source_id` | `{action: "stopped"}` |
| `step.cdc_status` | `source_id` | `{state, provider, tables, lag}` |
| `step.cdc_snapshot` | `source_id`, `tables` | `{action: "snapshot_triggered"}` |
| `step.cdc_schema_history` | `source_id`, `table` | `{versions: [...], count}` |

### Tenancy Steps

| Step | Config | Output |
|------|--------|--------|
| `step.tenant_provision` | `tenantId` | `{status: "provisioned", schema}` |
| `step.tenant_deprovision` | `tenantId`, `mode` | `{status: "deprovisioned"}` |
| `step.tenant_migrate` | `tenantIds`, `parallelism`, `failureThreshold` | `{status, tenants, count, failed}` |

### Lakehouse Steps

| Step | Config | Output |
|------|--------|--------|
| `step.lakehouse_create_table` | `catalog`, `namespace`, `table`, `schema` | `{status, tableUUID}` |
| `step.lakehouse_evolve_schema` | `catalog`, `namespace`, `table`, `changes` | `{status, schemaId}` |
| `step.lakehouse_write` | `catalog`, `namespace`, `table`, `data`, `mode` | `{status, recordCount}` |
| `step.lakehouse_compact` | `catalog`, `namespace`, `table` | `{status}` |
| `step.lakehouse_snapshot` | `catalog`, `namespace`, `table`, `action` | varies by action |
| `step.lakehouse_query` | `catalog`, `namespace`, `table` | `{snapshots, schema}` |
| `step.lakehouse_expire_snapshots` | `catalog`, `namespace`, `table`, `olderThan` | `{status, expiredCount}` |

### Time-Series Steps (shared)

| Step | Config | Output |
|------|--------|--------|
| `step.ts_write` | `module`, `measurement`, `tags`, `fields`, `timestamp` | `{status: "written"}` |
| `step.ts_write_batch` | `module`, `points` | `{status: "written", count}` |
| `step.ts_query` | `module`, `query` | `{rows: [...], count}` |
| `step.ts_downsample` | `module`, `source`, `target`, `aggregation`, `interval` | `{status, query}` |
| `step.ts_retention` | `module`, `duration` | `{status: "updated"}` |
| `step.ts_continuous_query` | `module`, `viewName`, `query`, `refreshInterval`, `action` | `{status, viewName}` |

### Druid Steps

| Step | Config | Output |
|------|--------|--------|
| `step.ts_druid_ingest` | `module`, `spec` | `{status, supervisorId}` |
| `step.ts_druid_query` | `module`, `query`, `queryType` | `{rows: [...], count}` |
| `step.ts_druid_datasource` | `module`, `datasource`, `action` | varies by action |
| `step.ts_druid_compact` | `module`, `datasource` | `{status}` |

### Schema Registry Steps

| Step | Config | Output |
|------|--------|--------|
| `step.schema_register` | `registry`, `subject`, `schema`, `schemaType` | `{schemaId, version}` |
| `step.schema_validate` | `registry`, `subject`, `data` | `{valid, errors}` |

### Graph Steps

| Step | Config | Output |
|------|--------|--------|
| `step.graph_query` | `module`, `cypher`, `params` | `{rows: [...], count}` |
| `step.graph_write` | `module`, `nodes`, `relationships` | `{nodesCreated, relationshipsCreated}` |
| `step.graph_import` | `module`, `source`, `mapping` | `{imported, nodeLabel}` |
| `step.graph_extract_entities` | `text`, `types` | `{entities: [...], count}` |
| `step.graph_link` | `module`, `from`, `to`, `type` | `{linked}` |

### Catalog Steps

| Step | Config | Output |
|------|--------|--------|
| `step.catalog_register` | `catalog`, `dataset`, `schema`, `owner` | `{status: "registered"}` |
| `step.catalog_search` | `catalog`, `query`, `limit` | `{results: [...], total}` |
| `step.contract_validate` | `contract`, `database` | `{passed, schemaOk, qualityOk}` |

### Data Quality Steps

| Step | Config | Output |
|------|--------|--------|
| `step.quality_check` | `module`, `table`, `checks` | `{passed, results: [...]}` |
| `step.quality_schema_validate` | `module`, `contract`, `data` | `{valid, errors}` |
| `step.quality_profile` | `module`, `table`, `columns` | `{rowCount, columns: {...}}` |
| `step.quality_compare` | `module`, `baseline`, `current`, `tolerances` | `{passed, diffs}` |
| `step.quality_anomaly` | `module`, `table`, `columns`, `method`, `threshold` | `{results: [...]}` |
| `step.quality_dbt_test` | `project`, `select` | `{passed, results}` |
| `step.quality_soda_check` | `config`, `checksFile` | `{passed, results}` |
| `step.quality_ge_validate` | `checkpoint` | `{passed, results}` |

### Migration Steps

| Step | Config | Output |
|------|--------|--------|
| `step.migrate_plan` | `module` | `{plan: [...], safe, changeCount}` |
| `step.migrate_apply` | `module`, `plan`, `mode` | `{status: "applied", changesApplied}` |
| `step.migrate_run` | `module`, `version` | `{status: "migrated", version}` |
| `step.migrate_rollback` | `module`, `steps` | `{status, fromVersion, toVersion}` |
| `step.migrate_status` | `module` | `{version, pending, applied}` |

## Trigger Types

### `trigger.cdc`

Fires a workflow when a CDC change event is captured.

```yaml
pipelines:
  - name: on_user_change
    trigger:
      type: cdc
      config:
        source_id: aurora-users
        tables: [public.users]
        actions: [INSERT, UPDATE]
    steps:
      - name: process
        type: step.set
        config:
          operation: "${ trigger.action }"
          user_id: "${ trigger.data.id }"
```

## Example: CDC to Lakehouse Pipeline

```yaml
modules:
  - name: cdc_source
    type: cdc.source
    config:
      provider: bento
      source_id: users-cdc
      source_type: postgres
      connection: "${ config(\"aurora_dsn\") }"
      tables: [public.users]

  - name: catalog
    type: catalog.iceberg
    config:
      endpoint: "${ config(\"iceberg_url\") }"
      warehouse: "s3://lake/warehouse"
      credential: "${ config(\"iceberg_token\") }"

  - name: quality
    type: quality.checks
    config:
      provider: builtin

pipelines:
  - name: cdc_to_lakehouse
    trigger:
      type: cdc
      config:
        source_id: users-cdc
        tables: [public.users]
    steps:
      - name: validate
        type: step.quality_check
        config:
          checks:
            - { type: not_null, columns: [id, email] }

      - name: write
        type: step.lakehouse_write
        config:
          catalog: catalog
          namespace: [raw]
          table: users
          mode: upsert
```

## Data Contracts

Define quality expectations in YAML:

```yaml
# contracts/users.yaml
dataset: raw.users
owner: data-team
schema:
  columns:
    - { name: id, type: bigint, nullable: false }
    - { name: email, type: varchar, nullable: false, pattern: "^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$" }
quality:
  - { type: freshness, maxAge: 1h }
  - { type: row_count, min: 1000 }
  - { type: not_null, columns: [id, email] }
  - { type: unique, columns: [id] }
```

Validate with `step.contract_validate`:

```yaml
- name: check_contract
  type: step.contract_validate
  config:
    contract: contracts/users.yaml
    database: my-database
```

## Schema Migration

### Declarative (desired-state diffing)

```yaml
# schemas/users.yaml
table: users
columns:
  - { name: id, type: bigint, primaryKey: true }
  - { name: email, type: "varchar(255)", nullable: false, unique: true }
  - { name: created_at, type: timestamptz, default: "now()" }
indexes:
  - { columns: [email], unique: true }
```

Non-breaking changes (add column, add index, widen type) apply automatically. Breaking changes respect `onBreakingChange` policy.

### Scripted (numbered migrations)

```
migrations/
  001_create_users.up.sql
  001_create_users.down.sql
  002_add_email.up.sql
  002_add_email.down.sql
```

## Build

```bash
go build -o workflow-plugin-data-engineering ./cmd/workflow-plugin-data-engineering
go test ./... -race
```

## License

Commercial - GoCodeAlone
