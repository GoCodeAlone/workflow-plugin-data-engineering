// Command workflow-plugin-data-engineering is a workflow engine external plugin
// providing CDC, lakehouse, time-series, graph, data quality, and multi-tenancy
// capabilities. It runs as a subprocess communicating with the host workflow
// engine via the go-plugin gRPC protocol.
package main

import (
	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

var version = "dev"

func main() {
	sdk.Serve(internal.NewDataEngineeringPlugin(version))
}
