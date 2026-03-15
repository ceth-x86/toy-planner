package optimizer

import (
	"testing"
	"toy-optimizer/pkg/catalog"
)

func TestCalculateSelectivity(t *testing.T) {
	cat := catalog.NewCatalog()
	cat.RegisterTable(catalog.TableMetadata{
		Name:     "Users",
		RowCount: 1000,
		ColumnNDVs: map[string]int{
			"id":     1000, // Unique
			"city":   10,   // Low NDV
			"status": 5,
		},
	})

	tests := []struct {
		name      string
		condition string
		expected  float64
	}{
		{
			name:      "Equality Unique",
			condition: "Users.id = 42",
			expected:  0.001, // 1/1000
		},
		{
			name:      "Equality Non-Unique",
			condition: "Users.city = 'London'",
			expected:  0.1, // 1/10
		},
		{
			name:      "Inequality Greater",
			condition: "Users.id > 100",
			expected:  0.33,
		},
		{
			name:      "Inequality Less-Equal",
			condition: "Users.id <= 500",
			expected:  0.33,
		},
		{
			name:      "Not Equal",
			condition: "Users.status != 'active'",
			expected:  0.8, // 1 - 1/5 = 0.8
		},
		{
			name:      "Default (no table)",
			condition: "NonExistent.id = 1",
			expected:  1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateSelectivity(tt.condition, cat)
			// Using small epsilon for float comparison
			if (got > tt.expected+0.0001) || (got < tt.expected-0.0001) {
				t.Errorf("%s: CalculateSelectivity() = %v, want %v", tt.name, got, tt.expected)
			}
		})
	}
}
