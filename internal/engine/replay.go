package engine

import (
	"fmt"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/materialize"
)

func (d *DB) apply(r journal.Record) error {
	if r.Sequence != d.sequence+1 || r.Format != 1 {
		return fmt.Errorf("invalid record sequence")
	}
	switch r.Kind {
	case "relation":
		if r.Relation == nil || r.Relation.Created != r.Sequence {
			return fmt.Errorf("invalid relation record")
		}
		if err := r.Relation.Validate(); err != nil {
			return err
		}
		if _, ok := d.catalog.Relations[r.Relation.Name]; ok {
			return fmt.Errorf("duplicate relation")
		}
		d.catalog.Relations[r.Relation.Name] = *r.Relation
		d.store.Ensure(r.Relation.Name)
	case "index":
		if r.Index == nil || r.Index.Created != r.Sequence {
			return fmt.Errorf("invalid index record")
		}
		if err := d.catalog.CheckIndex(*r.Index); err != nil {
			return err
		}
		d.catalog.Indexes[r.Index.Name] = *r.Index
	case "view":
		if r.View == nil || r.View.Created != r.Sequence {
			return fmt.Errorf("invalid view record")
		}
		definition := r.View.WithDefaults()
		if _, exists := d.views.Views[definition.Name]; exists {
			return fmt.Errorf("duplicate view")
		}
		if err := materialize.ValidateDefinition(d.catalog, definition); err != nil {
			return err
		}
		if err := d.views.Register(definition, d.catalog, d.store); err != nil {
			return err
		}
	case "commit":
		seen := map[string]bool{}
		grouped := map[string][]string{}
		for _, m := range r.Writes {
			relation, err := d.catalog.Relation(m.Relation)
			if err != nil {
				return err
			}
			token := m.Relation + "\x00" + m.Key
			if seen[token] {
				return fmt.Errorf("duplicate mutation")
			}
			seen[token] = true
			grouped[m.Relation] = append(grouped[m.Relation], m.Key)
			if !m.Deleted {
				row, err := relation.Normalize(m.Row)
				if err != nil {
					return err
				}
				if relation.Key(row) != m.Key {
					return fmt.Errorf("stored primary mismatch")
				}
			}
		}
		// Capture pre-commit rows against the prior sequence, then apply the
		// source writes and maintain derived views in the same state-machine
		// step so both live publish and journal replay take one identical path.
		before := map[string]map[string]datum.Row{}
		for relation, keys := range grouped {
			before[relation] = d.views.Capture(d.store, relation, keys, d.sequence)
		}
		d.store.Apply(r.Sequence, r.Writes)
		if err := d.views.Maintain(r.Sequence, r.Writes, before, d.catalog, d.store); err != nil {
			return err
		}
	case "vacuum":
		if r.Horizon < d.horizon || r.Horizon > d.sequence {
			return fmt.Errorf("invalid vacuum horizon")
		}
		d.store.Vacuum(r.Horizon)
		d.views.Prune(r.Horizon)
		d.horizon = r.Horizon
	default:
		return fmt.Errorf("unknown record kind %s", r.Kind)
	}
	d.sequence = r.Sequence
	// Every durable record advances the source head. Commit records maintain
	// eager views in their own case above; every other record kind (relation,
	// index, view, vacuum) changes no source rows but still consumes a
	// sequence, so eager view heads must be rebound to it and never trail.
	if r.Kind != "commit" {
		d.views.Advance(r.Sequence)
	}
	return nil
}
func overlay(rows map[string]datum.Row, writes map[string]map[string]staged, table string) {
	for key, m := range writes[table] {
		if m.Mutation.Deleted {
			delete(rows, key)
		} else {
			rows[key] = datum.Clone(m.Mutation.Row)
		}
	}
}
