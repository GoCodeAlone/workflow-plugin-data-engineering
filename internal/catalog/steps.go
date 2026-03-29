package catalog

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// schemaRegisterStep implements step.schema_register — registers a new schema version.
type schemaRegisterStep struct {
	name string
}

// NewSchemaRegisterStep creates a new step.schema_register instance.
func NewSchemaRegisterStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &schemaRegisterStep{name: name}, nil
}

func (s *schemaRegisterStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	registry, _ := strValCatalog(config, "registry")
	if registry == "" {
		return nil, fmt.Errorf("step.schema_register %q: registry is required", s.name)
	}
	subject, _ := strValCatalog(config, "subject")
	if subject == "" {
		return nil, fmt.Errorf("step.schema_register %q: subject is required", s.name)
	}
	schemaStr, _ := strValCatalog(config, "schema")
	if schemaStr == "" {
		return nil, fmt.Errorf("step.schema_register %q: schema is required", s.name)
	}
	schemaType, _ := strValCatalog(config, "schemaType")

	mod, err := LookupSRModule(registry)
	if err != nil {
		return nil, fmt.Errorf("step.schema_register %q: %w", s.name, err)
	}

	def := SchemaDefinition{
		Schema:     schemaStr,
		SchemaType: schemaType,
	}
	if refs, ok := config["references"].([]any); ok {
		for _, r := range refs {
			if rm, ok := r.(map[string]any); ok {
				ref := SchemaReference{}
				if n, ok := rm["name"].(string); ok {
					ref.Name = n
				}
				if sub, ok := rm["subject"].(string); ok {
					ref.Subject = sub
				}
				if v, ok := rm["version"].(int); ok {
					ref.Version = v
				}
				def.References = append(def.References, ref)
			}
		}
	}

	schemaID, err := mod.Client().RegisterSchema(ctx, subject, def)
	if err != nil {
		return nil, fmt.Errorf("step.schema_register %q: %w", s.name, err)
	}

	// Fetch the version number from the registry.
	info, err := mod.Client().GetLatestSchema(ctx, subject)
	if err != nil {
		// Non-fatal: still return the ID.
		return &sdk.StepResult{Output: map[string]any{
			"schemaId": schemaID,
			"subject":  subject,
		}}, nil
	}

	return &sdk.StepResult{Output: map[string]any{
		"schemaId": schemaID,
		"subject":  subject,
		"version":  info.Version,
	}}, nil
}

// schemaValidateStep implements step.schema_validate — validates data against a registered schema.
type schemaValidateStep struct {
	name string
}

// NewSchemaValidateStep creates a new step.schema_validate instance.
func NewSchemaValidateStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &schemaValidateStep{name: name}, nil
}

func (s *schemaValidateStep) Execute(ctx context.Context, _ map[string]any, _ map[string]map[string]any, _, _, config map[string]any) (*sdk.StepResult, error) {
	registry, _ := strValCatalog(config, "registry")
	if registry == "" {
		return nil, fmt.Errorf("step.schema_validate %q: registry is required", s.name)
	}
	subject, _ := strValCatalog(config, "subject")
	if subject == "" {
		return nil, fmt.Errorf("step.schema_validate %q: subject is required", s.name)
	}
	data, _ := strValCatalog(config, "data")
	if data == "" {
		return nil, fmt.Errorf("step.schema_validate %q: data is required", s.name)
	}

	mod, err := LookupSRModule(registry)
	if err != nil {
		return nil, fmt.Errorf("step.schema_validate %q: %w", s.name, err)
	}

	info, err := mod.Client().GetLatestSchema(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("step.schema_validate %q: fetch schema: %w", s.name, err)
	}

	schemaDef := SchemaDefinition{
		Schema:     info.Schema,
		SchemaType: info.SchemaType,
	}

	validationErr := mod.Client().ValidateSchema(ctx, schemaDef, []byte(data))
	if validationErr != nil {
		return &sdk.StepResult{Output: map[string]any{
			"valid":   false,
			"errors":  []string{validationErr.Error()},
			"subject": subject,
		}}, nil
	}

	return &sdk.StepResult{Output: map[string]any{
		"valid":   true,
		"errors":  []string{},
		"subject": subject,
	}}, nil
}

func strValCatalog(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
