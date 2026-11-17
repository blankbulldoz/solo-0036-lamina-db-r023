// Package materialize owns user-declared, fixed single-relation aggregate
// materialized views: their definitions, derived state, commit-bounded
// maintenance, history retention and source/result verification.
//
// A view is fully derived from durable source records. Only its definition is
// journaled; every derived value can be recomputed from the version store, so
// replay and restore reconstruct identical state without a manual refresh.
package materialize

import (
	"errors"
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
)

// Sentinel errors let the transport layer map failures to the service's
// bounded error vocabulary without matching message text.
var (
	ErrNotFound     = errors.New("materialized view not found")
	ErrSnapshotGone = errors.New("materialized view snapshot unavailable")
)

// Measure declares one aggregate output of a view.
type Measure struct {
	Function string `json:"function"`
	Column   string `json:"column,omitempty"`
}

// Predicate is a fixed AND-connected filter evaluated against source rows.
// It mirrors the supported query predicate operators without importing query.
type Predicate struct {
	Column string      `json:"column"`
	Op     string      `json:"op"`
	Value  datum.Value `json:"value"`
}

// Definition is the immutable, durable declaration of a materialized view.
type Definition struct {
	Name     string      `json:"name"`
	Relation string      `json:"relation"`
	Measures []Measure   `json:"measures"`
	Where    []Predicate `json:"where,omitempty"`
	Refresh  string      `json:"refresh"`
	History  string      `json:"history"`
	Created  uint64      `json:"created"`
}

const (
	RefreshOnCommit = "on_commit"
	RefreshOnDemand = "on_demand"
	HistoryRetained = "retained"
	HistoryHeadOnly = "head_only"
	StatusActive    = "active"
	StatusOverflow  = "overflow"

	maxMeasures = 16
	maxFilters  = 32
)

// WithDefaults fills the policy defaults so durable records are always explicit.
func (d Definition) WithDefaults() Definition {
	if d.Refresh == "" {
		d.Refresh = RefreshOnCommit
	}
	if d.History == "" {
		d.History = HistoryRetained
	}
	return d
}

// OutputKey is the JSON key of a measure inside a derived value row.
func (m Measure) OutputKey() (string, error) {
	switch m.Function {
	case "count":
		if m.Column != "" && m.Column != "*" {
			return "", fmt.Errorf("count only supports count(*)")
		}
		return "count", nil
	case "sum":
		if m.Column == "" || m.Column == "*" {
			return "", fmt.Errorf("sum requires an integer column")
		}
		return "sum_" + m.Column, nil
	default:
		return "", fmt.Errorf("unsupported aggregate %q", m.Function)
	}
}

// ValidateDefinition checks a declaration against the catalog it will derive
// from. It never mutates catalog state.
func ValidateDefinition(c *catalog.Catalog, d Definition) error {
	if !catalog.ValidName(d.Name) {
		return fmt.Errorf("invalid view name")
	}
	if d.Refresh != RefreshOnCommit && d.Refresh != RefreshOnDemand {
		return fmt.Errorf("refresh must be %q or %q", RefreshOnCommit, RefreshOnDemand)
	}
	if d.History != HistoryRetained && d.History != HistoryHeadOnly {
		return fmt.Errorf("history must be %q or %q", HistoryRetained, HistoryHeadOnly)
	}
	r, err := c.Relation(d.Relation)
	if err != nil {
		return err
	}
	if len(d.Measures) == 0 || len(d.Measures) > maxMeasures {
		return fmt.Errorf("view needs 1..%d measures", maxMeasures)
	}
	seen := map[string]bool{}
	for _, m := range d.Measures {
		key, err := m.OutputKey()
		if err != nil {
			return err
		}
		if seen[key] {
			return fmt.Errorf("duplicate measure %s", key)
		}
		seen[key] = true
		if m.Function == "sum" {
			col, ok := r.Column(m.Column)
			if !ok {
				return fmt.Errorf("unknown sum column %s", m.Column)
			}
			if col.Type != "int" {
				return fmt.Errorf("sum requires integer column %s", m.Column)
			}
		}
	}
	if len(d.Where) > maxFilters {
		return fmt.Errorf("view filter limit %d exceeded", maxFilters)
	}
	for _, p := range d.Where {
		col, ok := r.Column(p.Column)
		if !ok {
			return fmt.Errorf("unknown filter column %s", p.Column)
		}
		switch p.Op {
		case "=", "!=", "<", ">", "<=", ">=", "is_null", "not_null":
		default:
			return fmt.Errorf("unsupported filter operator %s", p.Op)
		}
		if p.Op != "is_null" && p.Op != "not_null" && !p.Value.Null() && p.Value.Kind != col.Type {
			return fmt.Errorf("filter type mismatch for %s", p.Column)
		}
	}
	return nil
}

// matches evaluates the fixed filter of a view. A nil predicate list matches
// every row, matching the structured query AND semantics.
func matches(row datum.Row, predicates []Predicate) bool {
	for _, p := range predicates {
		v := row[p.Column]
		if p.Op == "is_null" {
			if !v.Null() {
				return false
			}
			continue
		}
		if p.Op == "not_null" {
			if v.Null() {
				return false
			}
			continue
		}
		if v.Null() || p.Value.Null() {
			return false
		}
		n, err := v.Compare(p.Value)
		if err != nil {
			return false
		}
		pass := false
		switch p.Op {
		case "=":
			pass = n == 0
		case "!=":
			pass = n != 0
		case "<":
			pass = n < 0
		case ">":
			pass = n > 0
		case "<=":
			pass = n <= 0
		case ">=":
			pass = n >= 0
		}
		if !pass {
			return false
		}
	}
	return true
}
