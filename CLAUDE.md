# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

## Project Overview

`workflow-plugin-data-engineering` is a gRPC external plugin for the [GoCodeAlone/workflow](https://github.com/GoCodeAlone/workflow) engine. It provides data engineering capabilities: CDC (Change Data Capture), lakehouse table management, time-series ingestion, graph databases, data quality checks, and multi-tenancy.

## Build & Run

```sh
# Build
go build -o workflow-plugin-data-engineering ./cmd/workflow-plugin-data-engineering

# Test
go test ./...
go test -race ./...

# Cross-build (example)
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/workflow-plugin-data-engineering
```

## Project Structure

```
cmd/workflow-plugin-data-engineering/  — Plugin entry point (sdk.Serve)
internal/
  plugin.go         — PluginProvider, ModuleProvider, StepProvider, TriggerProvider, SchemaProvider
  cdc/              — CDC provider interface + Bento/Debezium/DMS implementations
  tenancy/          — Multi-tenant data isolation (schema/database strategies)
plugin.json         — Plugin manifest (public, MIT)
.goreleaser.yml     — GoReleaser v2 config (linux+darwin, amd64+arm64, CGO_ENABLED=0)
```

## Module and Step Types

| Type | Description |
|------|-------------|
| `cdc.source` | CDC stream from Postgres/MySQL (provider: bento, debezium, dms) |
| `data.tenancy` | Multi-tenant schema/database isolation and provisioning |
| `step.cdc_start` | Start a CDC source stream |
| `step.cdc_stop` | Stop a CDC source stream |
| `step.cdc_status` | Get CDC stream status |
| `step.cdc_snapshot` | Trigger full table snapshot |
| `step.cdc_schema_history` | Query DDL change history for a table |
| `step.tenant_provision` | Provision a new tenant (create schema/database) |
| `step.tenant_deprovision` | Remove a tenant (drop schema/database) |
| `step.tenant_migrate` | Run schema migrations for a tenant |
| `trigger.cdc` | Fire workflow on CDC change events |

## Key Conventions

- Use `${ }` expr syntax (not `{{ }}` Go templates) in workflow config examples.
- All modules use `sync.RWMutex` for thread safety.
- Config structs have both `json` and `yaml` struct tags.
- Error messages include the module/step name.
- SDK import: `sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"`
- Handshake magic: `WORKFLOW_PLUGIN=workflow-external-plugin-v1`
- CI uses ubuntu-latest runners with `RELEASES_TOKEN` secret for private dependency access.
- `CGO_ENABLED=0` for all builds.

## Adding a New Module Type

1. Create `internal/<domain>/module.go` implementing `sdk.ModuleInstance`.
2. Add the type name to `ModuleTypes()` in `internal/plugin.go`.
3. Add a case to `CreateModule()` in `internal/plugin.go`.
4. Add schema to `ModuleSchemas()` in `internal/plugin.go`.
5. Add to `plugin.json` capabilities.

## Adding a New Step Type

1. Create the step struct implementing `sdk.StepInstance` in `internal/<domain>/steps.go`.
2. Add the type name to `StepTypes()` in `internal/plugin.go`.
3. Add a case to `CreateStep()` in `internal/plugin.go`.
4. Add to `plugin.json` capabilities.

## Links

- [Workflow Engine](https://github.com/GoCodeAlone/workflow)
- [Plugin SDK](https://github.com/GoCodeAlone/workflow/tree/main/plugin/external/sdk)
- [Design Doc](https://github.com/GoCodeAlone/workflow/blob/main/docs/plans/2026-03-28-data-engineering-plugin-design.md)
