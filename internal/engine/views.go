package engine

import (
	"errors"
	"lamina/internal/catalog"
	"lamina/internal/journal"
	"lamina/internal/materialize"
)

func validViewName(name string) bool { return catalog.ValidName(name) }

// classifyViewError maps materialize package sentinels onto the service's
// bounded error vocabulary.
func classifyViewError(err error) error {
	switch {
	case errors.Is(err, materialize.ErrNotFound):
		return fault("not_found", "%s", err.Error())
	case errors.Is(err, materialize.ErrSnapshotGone):
		return fault("snapshot", "%s", err.Error())
	default:
		return invalid(err)
	}
}

// ViewInfo is the public catalog and maintenance explanation of one view.
type ViewInfo struct {
	Name            string                  `json:"name"`
	Relation        string                  `json:"relation"`
	Measures        []materialize.Measure   `json:"measures"`
	Where           []materialize.Predicate `json:"where,omitempty"`
	Refresh         string                  `json:"refresh"`
	History         string                  `json:"history"`
	Created         uint64                  `json:"created"`
	Status          string                  `json:"status"`
	SourceSequence  uint64                  `json:"source_sequence"`
	Watermark       uint64                  `json:"watermark"`
	HistoryPoints   int                     `json:"history_points"`
	OverflowColumns []string                `json:"overflow_columns,omitempty"`
}

// ViewAnswer is a derived aggregate row bound to one exact source commit.
type ViewAnswer struct {
	Name           string         `json:"name"`
	RequestedAt    uint64         `json:"requested_at"`
	SourceSequence uint64         `json:"source_sequence"`
	Status         string         `json:"status"`
	Row            map[string]any `json:"row"`
	Overflow       []string       `json:"overflow_columns,omitempty"`
	Materialized   bool           `json:"materialized"`
	CaughtUp       bool           `json:"caught_up,omitempty"`
}

func toInfo(v *materialize.View) ViewInfo {
	status := materialize.StatusActive
	if v.Overflow {
		status = materialize.StatusOverflow
	}
	return ViewInfo{
		Name:            v.Def.Name,
		Relation:        v.Def.Relation,
		Measures:        append([]materialize.Measure(nil), v.Def.Measures...),
		Where:           append([]materialize.Predicate(nil), v.Def.Where...),
		Refresh:         v.Def.Refresh,
		History:         v.Def.History,
		Created:         v.Def.Created,
		Status:          status,
		SourceSequence:  v.Head.Sequence,
		Watermark:       v.Watermark,
		HistoryPoints:   len(v.Chain),
		OverflowColumns: append([]string(nil), v.OverflowColumns...),
	}
}

// CreateView validates, durably registers and builds a materialized view. The
// registration record is the only durable view fact: a failed publish leaves
// no registered view, and a successful one always has an explainable result.
func (d *DB) CreateView(definition materialize.Definition) (ViewInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return ViewInfo{}, err
	}
	definition = definition.WithDefaults()
	if !validViewName(definition.Name) {
		return ViewInfo{}, fault("invalid", "invalid view name")
	}
	if _, exists := d.views.Views[definition.Name]; exists {
		return ViewInfo{}, fault("conflict", "view already exists")
	}
	if err := materialize.ValidateDefinition(d.catalog, definition); err != nil {
		return ViewInfo{}, invalid(err)
	}
	definition.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "view", View: &definition}); err != nil {
		return ViewInfo{}, err
	}
	v, ok := d.views.Views[definition.Name]
	if !ok {
		return ViewInfo{}, fault("unavailable", "view registration missing after publish")
	}
	return toInfo(v), nil
}

// ListViews explains every view's source position and retention cost.
func (d *DB) ListViews() ([]ViewInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	out := make([]ViewInfo, 0, len(d.views.Views))
	for _, v := range d.views.List() {
		out = append(out, toInfo(v))
	}
	return out, nil
}

// ReadView answers a view at a retained snapshot. A nil at means the current
// source sequence. Every answer carries the exact source sequence it
// corresponds to; a lazy view may recompute from source rather than serving an
// unmaintained head.
func (d *DB) ReadView(name string, at *uint64) (ViewAnswer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return ViewAnswer{}, err
	}
	requested := d.sequence
	if at != nil {
		requested = *at
	}
	if requested < d.horizon || requested > d.sequence {
		return ViewAnswer{}, fault("snapshot", "snapshot unavailable; retained interval %d..%d", d.horizon, d.sequence)
	}
	result, err := d.views.ReadAt(name, requested, d.sequence, d.catalog, d.store)
	if err != nil {
		return ViewAnswer{}, classifyViewError(err)
	}
	d.viewReads++
	row := map[string]any{}
	for k, value := range result.Point.Row {
		row[k] = value
	}
	return ViewAnswer{
		Name:           name,
		RequestedAt:    requested,
		SourceSequence: result.SourceSequence,
		Status:         result.Status,
		Row:            row,
		Overflow:       result.Overflow,
		Materialized:   result.Materialized,
		CaughtUp:       result.CaughtUp,
	}, nil
}

// VerifyView recomputes one view from source and compares it with the
// maintained state, providing an explicit source/result correspondence check.
func (d *DB) VerifyView(name string) (ViewAnswer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return ViewAnswer{}, err
	}
	result, err := d.views.VerifyOne(name, d.catalog, d.store, d.sequence)
	if err != nil {
		return ViewAnswer{}, classifyViewError(err)
	}
	d.viewReads++
	row := map[string]any{}
	for k, value := range result.Point.Row {
		row[k] = value
	}
	return ViewAnswer{
		Name:           name,
		RequestedAt:    d.sequence,
		SourceSequence: d.sequence,
		Status:         result.Status,
		Row:            row,
		Overflow:       result.Overflow,
		Materialized:   result.Materialized,
		CaughtUp:       result.CaughtUp,
	}, nil
}
