package catalog

import "sync"

// TableMetadata contains table statistics and schema info.
type TableMetadata struct {
	Name       string
	RowCount   int
	Columns    []string
	Indexes    []string
	ColumnNDVs map[string]int // NDV: Number of Distinct Values per column
}

// Catalog is a registry of table metadata.
type Catalog struct {
	mu     sync.RWMutex
	tables map[string]TableMetadata
}

// NewCatalog creates a new empty Catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		tables: make(map[string]TableMetadata),
	}
}

// RegisterTable adds a table to the catalog.
func (c *Catalog) RegisterTable(meta TableMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if meta.ColumnNDVs == nil {
		meta.ColumnNDVs = make(map[string]int)
	}
	c.tables[meta.Name] = meta
}

// GetTable retrieves table metadata by name.
func (c *Catalog) GetTable(name string) (TableMetadata, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	meta, ok := c.tables[name]
	return meta, ok
}
