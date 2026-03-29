package ident_test

import (
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/ident"
)

func TestValidate_Valid(t *testing.T) {
	cases := []string{"users", "_table", "my_schema_1", "A", "_123"}
	for _, id := range cases {
		if err := ident.Validate(id); err != nil {
			t.Errorf("Validate(%q): unexpected error: %v", id, err)
		}
	}
}

func TestValidate_Invalid(t *testing.T) {
	cases := []string{"1bad", "has space", "dash-name", "semi;colon", "dot.name", "quote'"}
	for _, id := range cases {
		if err := ident.Validate(id); err == nil {
			t.Errorf("Validate(%q): expected error, got nil", id)
		}
	}
}

func TestValidate_Empty(t *testing.T) {
	if err := ident.Validate(""); err == nil {
		t.Error("Validate(\"\"): expected error for empty string, got nil")
	}
}
