//go:build wasm

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

func main() {
	sdk.Run(sdk.NewProcessorFunc(
		sdk.Specification{Name: "lowercase-hubspot-properties", Version: "v1.0.0"},
		func(ctx context.Context, record opencdc.Record) (opencdc.Record, error) {
			logger := sdk.Logger(ctx).With().Str("processor", "lowercase-hubspot-properties").Logger()

			// Accessing the Payload.After as StructuredData
			afterData, ok := record.Payload.After.(opencdc.StructuredData)
			if !ok {
				logger.Error().Msg("Payload.After is not StructuredData")
				return record, fmt.Errorf("Payload.After is not StructuredData")
			}

			// Creating a new map to store lowercased keys
			loweredProperties := make(map[string]interface{})
			for key, value := range afterData {
				lowerKey := strings.ToLower(key)
				loweredProperties[lowerKey] = value
			}

			// Wrapping the modified map under "properties"
			propertiesPayload := map[string]interface{}{
				"properties": loweredProperties,
			}

			// Updating the record's Payload.After with the new structure
			record.Payload.After = opencdc.StructuredData(propertiesPayload)

			// Log the final state of the record
			finalJSON, _ := json.Marshal(record)
			logger.Trace().Str("finalRecord", string(finalJSON)).Msg("Final state of the record before returning")

			return record, nil
		},
	))
}
