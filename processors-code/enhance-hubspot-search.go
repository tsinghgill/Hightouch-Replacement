//go:build wasm

package main

import (
	"context"
	"encoding/json"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

func main() {
	sdk.Run(sdk.NewProcessorFunc(
		sdk.Specification{Name: "enhance-hubspot-search", Version: "v1.0.0"},
		func(ctx context.Context, record opencdc.Record) (opencdc.Record, error) {
			logger := sdk.Logger(ctx).With().Str("processor", "enhance-hubspot-search").Logger()
			logger.Trace().Msg("Starting to process record")

			// Check if email exists in the record's Payload.After
			email, exists := record.Payload.After.(opencdc.StructuredData)["EMAIL"]
			if !exists {
				logger.Trace().Msg("Email field does not exist, passing record through without changes")
				return record, nil // Early return if no email is found
			}

			// Prepare the search request body as a structured map
			searchRequest := map[string]interface{}{
				"filterGroups": []map[string]interface{}{
					{
						"filters": []map[string]interface{}{
							{
								"propertyName": "email",
								"operator":     "EQ",
								"value":        email, // Use the email from the record
							},
						},
					},
				},
				"properties": []string{"email", "firstname", "lastname", "phone"},
			}

			// Directly assign the structured map to Payload.After
			record.Payload.After.(opencdc.StructuredData)["searchHubspotContactByEmailRequestBody"] = searchRequest
			logger.Trace().Msg("Added searchHubspotContactByEmailRequestBody to Payload.After as structured data")

			// Optionally, log the entire record for debugging purposes
			if recordJSON, err := json.Marshal(record); err == nil {
				logger.Trace().Str("record", string(recordJSON)).Msg("Current state of the record after processing")
			} else {
				logger.Error().Err(err).Msg("Failed to marshal the entire record to JSON")
			}

			return record, nil
		},
	))
}
