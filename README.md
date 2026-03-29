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

## Real-World Use Cases

### CDC from Aurora/RDS to Iceberg Data Lake

**Problem:** Companies like BeeHero need real-time mirrors of operational databases in queryable data lakes. Traditional ETL batch jobs create stale data; manually wiring Debezium→Kafka→Flink→Iceberg requires an SRE squad.

**Solution with this plugin:**

```yaml
modules:
  - name: aurora_cdc
    type: cdc.source
    config:
      provider: bento
      source_id: aurora-users
      source_type: postgres
      connection: "${ config(\"aurora_dsn\") }"
      tables: [public.users, public.orders]
      options:
        slot_name: workflow_cdc
        publication: workflow_pub

  - name: lake_catalog
    type: catalog.iceberg
    config:
      endpoint: "${ config(\"iceberg_rest_url\") }"
      warehouse: "s3://data-lake/warehouse"
      credential: "${ config(\"iceberg_token\") }"

  - name: quality
    type: quality.checks
    config:
      provider: builtin

pipelines:
  - name: cdc_to_iceberg
    trigger:
      type: cdc
      config:
        source_id: aurora-users
        tables: [public.users]
    steps:
      - name: validate
        type: step.quality_check
        config:
          checks:
            - { type: not_null, columns: [id, email] }
            - { type: unique, columns: [id] }
      - name: write_lake
        type: step.lakehouse_write
        config:
          catalog: lake_catalog
          namespace: [raw]
          table: users
          mode: upsert
      - name: register
        type: step.schema_register
        config:
          registry: schema_reg
          subject: raw-users-value
          schemaType: JSON

  - name: lake_maintenance
    trigger:
      type: scheduler
      config:
        cron: "0 2 * * *"
    steps:
      - name: compact
        type: step.lakehouse_compact
        config:
          catalog: lake_catalog
          namespace: [raw]
          table: users
      - name: expire
        type: step.lakehouse_expire_snapshots
        config:
          catalog: lake_catalog
          namespace: [raw]
          table: users
          olderThan: 7d
```

Swap `provider: bento` for `provider: debezium` (enterprise Kafka Connect) or `provider: dms` (AWS-native) without changing the rest of the pipeline.

### Real-Time Analytics with ClickHouse + Druid

**Problem:** Companies like Mux process billions of events for real-time dashboards. They need sub-second query latency on streaming data with dual-write for different query patterns (OLAP vs real-time aggregation).

**Solution:**

```yaml
modules:
  - name: clickhouse
    type: timeseries.clickhouse
    config:
      endpoints: ["ch1:9000", "ch2:9000"]
      database: analytics
      username: "${ config(\"ch_user\") }"
      password: "${ config(\"ch_pass\") }"
      compression: lz4

  - name: druid
    type: timeseries.druid
    config:
      routerUrl: "http://druid-router:8888"

  - name: quality
    type: quality.checks
    config:
      provider: builtin

pipelines:
  - name: ingest_events
    trigger:
      type: http
      config:
        path: /api/v1/events
        method: POST
    steps:
      - name: write_clickhouse
        type: step.ts_write_batch
        config:
          module: clickhouse
          points: "${ body.events }"
      - name: ingest_druid
        type: step.ts_druid_ingest
        config:
          module: druid
          spec:
            type: kafka
            dataSchema:
              dataSource: events
              timestampSpec: { column: __time, format: auto }
              dimensionsSpec: { useSchemaDiscovery: true }
            ioConfig:
              topic: events-stream
              consumerProperties:
                bootstrap.servers: "kafka:9092"

  - name: anomaly_scan
    trigger:
      type: scheduler
      config:
        cron: "*/15 * * * *"
    steps:
      - name: query_recent
        type: step.ts_query
        config:
          module: clickhouse
          query: "SELECT host, avg(latency_ms) as avg_latency FROM events WHERE time > now() - INTERVAL 1 HOUR GROUP BY host"
      - name: detect
        type: step.quality_anomaly
        config:
          method: zscore
          threshold: 3.0
```

### Multi-Tenant SaaS Data Platform

**Problem:** SaaS companies need tenant-isolated data infrastructure that scales from free-tier shared tables to enterprise-grade dedicated schemas, with zero-downtime tenant onboarding and safe per-tenant migrations.

**Solution:**

```yaml
modules:
  - name: tenancy
    type: data.tenancy
    config:
      strategy: schema_per_tenant
      tenant_key: ctx.tenant_id
      schema_prefix: "tenant_"

  - name: migrations
    type: migrate.schema
    config:
      strategy: both
      schemas:
        - path: schemas/users.yaml
        - path: schemas/orders.yaml
      migrationsDir: ./migrations/
      lockTable: schema_migrations
      onBreakingChange: block

pipelines:
  - name: tenant_onboard
    trigger:
      type: http
      config:
        path: /api/v1/tenants
        method: POST
    steps:
      - name: provision
        type: step.tenant_provision
        config:
          tenantId: "${ body.tenant_id }"
      - name: migrate
        type: step.migrate_apply
        config:
          module: migrations
          mode: online
      - name: register_catalog
        type: step.catalog_register
        config:
          catalog: datahub
          dataset: "${ \"tenant_\" + body.tenant_id + \".users\" }"
          owner: "${ body.owner_email }"

  - name: tenant_schema_upgrade
    trigger:
      type: http
      config:
        path: /api/v1/tenants/migrate
        method: POST
    steps:
      - name: plan
        type: step.migrate_plan
        config:
          module: migrations
      - name: apply
        type: step.tenant_migrate
        config:
          tenantIds: "${ body.tenant_ids }"
          parallelism: 5
          failureThreshold: 3
```

### Knowledge Graph for AI/RAG

**Problem:** Companies like Precina Health and Cedars-Sinai build knowledge graphs from relational data for clinical decision support and research. GraphRAG improves answer accuracy 35% over vector-only RAG by providing structured context.

**Solution:**

```yaml
modules:
  - name: kg
    type: graph.neo4j
    config:
      uri: "bolt://neo4j:7687"
      database: knowledge
      username: "${ config(\"neo4j_user\") }"
      password: "${ config(\"neo4j_pass\") }"

  - name: catalog
    type: catalog.datahub
    config:
      endpoint: "http://datahub-gms:8080"
      token: "${ config(\"datahub_token\") }"

pipelines:
  - name: build_knowledge_graph
    trigger:
      type: http
      config:
        path: /api/v1/kg/ingest
        method: POST
    steps:
      - name: extract
        type: step.graph_extract_entities
        config:
          text: "${ body.content }"
          types: [person, org, location, email, date]
      - name: create_nodes
        type: step.graph_write
        config:
          module: kg
          nodes: "${ steps.extract.entities }"
      - name: link_entities
        type: step.graph_link
        config:
          module: kg
          from: { label: Person, key: name }
          to: { label: Organization, key: name }
          type: WORKS_AT

  - name: import_from_db
    trigger:
      type: http
      config:
        path: /api/v1/kg/import
        method: POST
    steps:
      - name: import
        type: step.graph_import
        config:
          module: kg
          source: "${ body.records }"
          mapping:
            nodeLabel: Patient
            properties:
              id: patient_id
              name: full_name
              diagnosis: primary_dx
```

### Data Contracts at Scale

**Problem:** GoCardless and similar companies need formal agreements between data producers and consumers, with tiered enforcement (block for critical data, warn for non-critical).

**Solution:**

```yaml
# contracts/payments.yaml
dataset: raw.payments
owner: payments-team
schema:
  columns:
    - { name: id, type: bigint, nullable: false }
    - { name: amount, type: "numeric(12,2)", nullable: false }
    - { name: currency, type: "varchar(3)", nullable: false, pattern: "^[A-Z]{3}$" }
    - { name: created_at, type: timestamptz, nullable: false }
quality:
  - { type: freshness, maxAge: 15m }
  - { type: row_count, min: 100 }
  - { type: not_null, columns: [id, amount, currency] }
  - { type: unique, columns: [id] }
  - { type: referential, column: customer_id, refTable: customers, refColumn: id }
```

```yaml
pipelines:
  - name: validate_payments
    trigger:
      type: scheduler
      config:
        cron: "*/30 * * * *"
    steps:
      - name: check
        type: step.contract_validate
        config:
          contract: contracts/payments.yaml
          database: payments-db
      - name: profile
        type: step.quality_profile
        config:
          table: raw.payments
          columns: [amount, currency]
      - name: alert_on_failure
        type: step.conditional
        config:
          field: steps.check.passed
          routes:
            "true": done
            "false": notify
      - name: notify
        type: step.publish
        config:
          topic: data-quality-alerts
          message: "${ json(steps.check) }"
      - name: done
        type: step.set
        config:
          status: healthy
```

### IoT Time-Series at Scale

**Problem:** Industrial IoT deployments generate millions of data points per second from thousands of sensors. Companies need fast ingestion, automatic downsampling, and anomaly detection — while legacy data historians can't keep up.

**Solution:**

```yaml
modules:
  - name: influx_hot
    type: timeseries.influxdb
    config:
      url: "${ config(\"influx_url\") }"
      token: "${ config(\"influx_token\") }"
      org: factory
      bucket: sensors-hot
      batchSize: 5000
      flushInterval: 100ms

  - name: tsdb
    type: timeseries.timescaledb
    config:
      connection: "${ config(\"timescale_dsn\") }"
      hypertables:
        - { table: sensor_data, timeColumn: ts, chunkInterval: 1h }

  - name: quality
    type: quality.checks
    config:
      provider: builtin

pipelines:
  - name: ingest_sensor_data
    trigger:
      type: http
      config:
        path: /api/v1/sensors/ingest
        method: POST
    steps:
      - name: write_hot
        type: step.ts_write_batch
        config:
          module: influx_hot
          points: "${ body.readings }"
      - name: write_tsdb
        type: step.ts_write_batch
        config:
          module: tsdb
          points: "${ body.readings }"

  - name: downsample_hourly
    trigger:
      type: scheduler
      config:
        cron: "5 * * * *"
    steps:
      - name: aggregate
        type: step.ts_downsample
        config:
          module: influx_hot
          source: sensor_data
          target: sensor_hourly
          aggregation: mean
          interval: 1h
      - name: continuous_agg
        type: step.ts_continuous_query
        config:
          module: tsdb
          viewName: sensor_hourly
          query: |
            SELECT time_bucket('1 hour', ts) AS bucket,
                   device_id, AVG(value) AS avg_val, MAX(value) AS max_val
            FROM sensor_data
            GROUP BY bucket, device_id
          refreshInterval: 30m
          action: create

  - name: anomaly_detection
    trigger:
      type: scheduler
      config:
        cron: "*/5 * * * *"
    steps:
      - name: query
        type: step.ts_query
        config:
          module: influx_hot
          query: |
            from(bucket: "sensors-hot")
              |> range(start: -1h)
              |> filter(fn: (r) => r._measurement == "sensor_data")
              |> mean()
      - name: detect
        type: step.quality_anomaly
        config:
          method: iqr
          threshold: 1.5

  - name: retention
    trigger:
      type: scheduler
      config:
        cron: "0 3 * * *"
    steps:
      - name: influx_retention
        type: step.ts_retention
        config:
          module: influx_hot
          duration: 7d
```

### Data Mesh with DataHub

**Problem:** Large organizations need domain-oriented data ownership with centralized discovery, lineage tracking, and governance — without requiring every team to build their own catalog infrastructure.

**Solution:**

```yaml
modules:
  - name: datahub
    type: catalog.datahub
    config:
      endpoint: "${ config(\"datahub_gms_url\") }"
      token: "${ config(\"datahub_token\") }"

  - name: schema_reg
    type: catalog.schema_registry
    config:
      endpoint: "${ config(\"sr_url\") }"
      defaultCompatibility: BACKWARD

pipelines:
  - name: register_data_product
    trigger:
      type: http
      config:
        path: /api/v1/catalog/register
        method: POST
    steps:
      - name: validate_contract
        type: step.contract_validate
        config:
          contract: "${ body.contract_path }"
          database: "${ body.database }"
      - name: register_schema
        type: step.schema_register
        config:
          registry: schema_reg
          subject: "${ body.dataset + \"-value\" }"
          schema: "${ body.schema }"
          schemaType: JSON
      - name: register_catalog
        type: step.catalog_register
        config:
          catalog: datahub
          dataset: "${ body.dataset }"
          schema: "${ body.schema }"
          owner: "${ body.owner }"

  - name: discover_datasets
    trigger:
      type: http
      config:
        path: /api/v1/catalog/search
        method: GET
    steps:
      - name: search
        type: step.catalog_search
        config:
          catalog: datahub
          query: "${ query.q }"
          limit: 20
```

## Known Gaps and Roadmap

Based on analysis of 2026 production use cases, these gaps are identified for future releases:

| Gap | Description | Priority |
|-----|-------------|----------|
| End-to-end schema evolution | Coordinated schema changes across CDC→Kafka schema→Iceberg (no tool does this today) | High |
| Expand-contract migrations | pgroll-style dual-version serving for zero-downtime DDL | High |
| Lineage tracking step | `step.catalog_lineage` to record upstream→downstream in DataHub/OpenMetadata | Medium |
| Hot/cold tier management | `step.ts_archive` to move aged data from TS DB to Parquet/S3 | Medium |
| LLM-powered entity extraction | Integrate with Claude/OpenAI for knowledge graph extraction beyond regex | Medium |
| ClickHouse materialized views | `step.ts_clickhouse_view` for MergeTree materialized view management | Low |
| Dynamic tenant tier promotion | Auto-promote tenants from shared→dedicated based on usage metrics | Low |
| CDC backpressure monitoring | WAL lag alerts and automatic throttling | Low |

## Build

```bash
go build -o workflow-plugin-data-engineering ./cmd/workflow-plugin-data-engineering
go test ./... -race
```

## License

Commercial - GoCodeAlone
