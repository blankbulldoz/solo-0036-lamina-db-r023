package materialize

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/versions"
	"math"
	"sort"
)

// Point is a derived aggregate row bound to one exact source sequence.
type Point struct {
	Sequence uint64           `json:"sequence"`
	Row      datum.Row        `json:"row"`
	NonNull  map[string]int64 `json:"non_null,omitempty"`
}

// View holds the durable definition plus reconstructed derived state.
type View struct {
	Def       Definition `json:"definition"`
	Head      Point      `json:"head"`
	Watermark uint64     `json:"watermark"`
	// Chain is retained history for history=retained views, ordered by
	// sequence and containing only points where derived values changed.
	Chain    []Point `json:"chain,omitempty"`
	Overflow bool    `json:"overflow"`
	// OverflowColumns records which sums stopped being meaningful.
	OverflowColumns []string `json:"overflow_columns,omitempty"`
}

// Manager owns every view's derived state.
type Manager struct {
	Views map[string]*View `json:"views"`
}

func NewManager() *Manager {
	return &Manager{Views: map[string]*View{}}
}

func (m *Manager) view(name string) (*View, error) {
	v, ok := m.Views[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return v, nil
}

// List returns views sorted by name for stable catalog output.
func (m *Manager) List() []*View {
	names := make([]string, 0, len(m.Views))
	for name := range m.Views {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*View, 0, len(names))
	for _, name := range names {
		out = append(out, m.Views[name])
	}
	return out
}

// VerifyOne recomputes one view from source state and fails closed on
// divergence. Lazy views deterministically catch their head up.
func (m *Manager) VerifyOne(name string, c *catalog.Catalog, s *versions.Store, currentSeq uint64) (ReadResult, error) {
	v, err := m.view(name)
	if err != nil {
		return ReadResult{}, err
	}
	if v.Def.Refresh == RefreshOnCommit {
		point, overflow, overflowColumns, err := compute(v.Def, currentSeq, c, s)
		if err != nil {
			return ReadResult{}, err
		}
		if v.Head.Sequence != currentSeq {
			return ReadResult{}, fmt.Errorf("view head %d trails source %d", v.Head.Sequence, currentSeq)
		}
		if v.Overflow != overflow || !sameStrings(v.OverflowColumns, overflowColumns) {
			return ReadResult{}, fmt.Errorf("view overflow state diverged from source")
		}
		if !overflow && !pointsEqual(v.Head, point) {
			return ReadResult{}, fmt.Errorf("view derived row diverged from source at %d", currentSeq)
		}
		status := StatusActive
		if overflow {
			status = StatusOverflow
		}
		return ReadResult{Definition: v.Def, Point: clonePoint(v.Head), Status: status, Overflow: overflowColumns, SourceSequence: currentSeq, Materialized: true}, nil
	}
	head, overflow, overflowColumns, err := compute(v.Def, currentSeq, c, s)
	if err != nil {
		return ReadResult{}, err
	}
	v.Head = head
	v.Overflow = overflow
	v.OverflowColumns = overflowColumns
	status := StatusActive
	if overflow {
		status = StatusOverflow
	}
	return ReadResult{Definition: v.Def, Point: clonePoint(head), Status: status, Overflow: overflowColumns, SourceSequence: currentSeq, CaughtUp: true, Materialized: false}, nil
}

// measureKeys returns the output keys in the declared order.
func measureKeys(d Definition) []string {
	keys := make([]string, 0, len(d.Measures))
	for _, measure := range d.Measures {
		key, _ := measure.OutputKey()
		keys = append(keys, key)
	}
	return keys
}

// compute evaluates a view against the full visible source state at one
// snapshot. The overflow flag reports an int64 sum that cannot be represented.
func compute(d Definition, at uint64, c *catalog.Catalog, s *versions.Store) (Point, bool, []string, error) {
	keys := measureKeys(d)
	point := Point{Sequence: at, Row: datum.Row{}, NonNull: map[string]int64{}}
	counts := map[string]int64{}
	sums := map[string]int64{}
	nonNull := map[string]int64{}
	overflow := map[string]bool{}
	for _, key := range keys {
		counts[key] = 0
		sums[key] = 0
		nonNull[key] = 0
	}
	if _, err := c.Relation(d.Relation); err != nil {
		return point, false, nil, err
	}
	for _, row := range s.Scan(d.Relation, at) {
		if !matches(row, d.Where) {
			continue
		}
		for _, measure := range d.Measures {
			key, _ := measure.OutputKey()
			if measure.Function == "count" {
				counts[key]++
				continue
			}
			value := row[measure.Column]
			if value.Null() {
				continue
			}
			nonNull[key]++
			if overflow[key] {
				continue
			}
			next := sums[key] + value.Int
			if value.Int > 0 && sums[key] > math.MaxInt64-value.Int || value.Int < 0 && sums[key] < math.MinInt64-value.Int {
				overflow[key] = true
				continue
			}
			sums[key] = next
		}
	}
	overflowNow := false
	var overflowColumns []string
	for _, measure := range d.Measures {
		key, _ := measure.OutputKey()
		switch measure.Function {
		case "count":
			point.Row[key] = datum.Integer(counts[key])
		case "sum":
			point.NonNull[key] = nonNull[key]
			if overflow[key] {
				overflowNow = true
				overflowColumns = append(overflowColumns, key)
				point.Row[key] = datum.Value{Kind: "null"}
			} else if nonNull[key] == 0 {
				point.Row[key] = datum.Value{Kind: "null"}
			} else {
				point.Row[key] = datum.Integer(sums[key])
			}
		}
	}
	sort.Strings(overflowColumns)
	return point, overflowNow, overflowColumns, nil
}

// Register installs a validated definition and builds its first derived point
// over the source state immediately before the registration record.
func (m *Manager) Register(d Definition, c *catalog.Catalog, s *versions.Store) error {
	if _, exists := m.Views[d.Name]; exists {
		return fmt.Errorf("view already exists")
	}
	point, overflow, overflowColumns, err := compute(d, d.Created-1, c, s)
	if err != nil {
		return err
	}
	v := &View{
		Def:             d,
		Head:            clonePoint(point),
		Watermark:       point.Sequence,
		Overflow:        overflow,
		OverflowColumns: overflowColumns,
	}
	if d.History == HistoryRetained {
		// The anchor must not alias Head: maintenance mutates Head in place.
		v.Chain = []Point{clonePoint(point)}
	}
	m.Views[d.Name] = v
	return nil
}

// Capture records the pre-commit visible rows for the keys a commit touches,
// so view maintenance can derive an exact delta before the store changes. The
// returned map is qualified by relation then datum key because different
// relations may share an identical datum key.
func (m *Manager) Capture(s *versions.Store, relation string, keys []string, at uint64) map[string]datum.Row {
	before := map[string]datum.Row{}
	affected := false
	for _, v := range m.Views {
		if v.Def.Relation == relation {
			affected = true
			break
		}
	}
	if !affected {
		return before
	}
	for _, key := range keys {
		if row, ok := s.Read(relation, key, at); ok {
			before[key] = row
		}
	}
	return before
}

func addSum(v *View, key string, current, delta int64) {
	if v.Overflow {
		for _, col := range v.OverflowColumns {
			if col == key {
				return
			}
		}
	}
	if delta > 0 && current > math.MaxInt64-delta || delta < 0 && current < math.MinInt64-delta {
		v.Overflow = true
		v.OverflowColumns = append(v.OverflowColumns, key)
		sort.Strings(v.OverflowColumns)
		v.Head.Row[key] = datum.Value{Kind: "null"}
		return
	}
	v.Head.Row[key] = datum.Integer(current + delta)
}

// Maintain advances every eager view across one committed source change set.
// It must run inside the same state-machine apply that publishes the writes,
// so the derived Head can never point at a different sequence than the source:
// after every commit each eager view's Head.Sequence equals the source head,
// even when the aggregate values themselves did not change.
func (m *Manager) Maintain(seq uint64, writes []versions.Mutation, before map[string]map[string]datum.Row, c *catalog.Catalog, s *versions.Store) error {
	byRelation := map[string][]versions.Mutation{}
	for _, w := range writes {
		byRelation[w.Relation] = append(byRelation[w.Relation], w)
	}
	for _, v := range m.Views {
		if v.Def.Refresh != RefreshOnCommit {
			continue
		}
		changed := false
		if touched, ok := byRelation[v.Def.Relation]; ok {
			prior := before[v.Def.Relation]
			for _, w := range touched {
				oldRow, hadOld := prior[w.Key]
				oldMatch := hadOld && matches(oldRow, v.Def.Where)
				newRow, hasNew := s.Read(v.Def.Relation, w.Key, seq)
				newMatch := hasNew && matches(newRow, v.Def.Where)
				if !oldMatch && !newMatch {
					continue
				}
				changed = true
				for _, measure := range v.Def.Measures {
					key, _ := measure.OutputKey()
					if measure.Function == "count" {
						current := v.Head.Row[key].Int
						if newMatch && !oldMatch {
							current++
						}
						if oldMatch && !newMatch {
							current--
						}
						v.Head.Row[key] = datum.Integer(current)
						continue
					}
					column := measure.Column
					if oldMatch {
						oldValue := oldRow[column]
						if !oldValue.Null() {
							v.Head.NonNull[key]--
							current := int64(0)
							if !v.Head.Row[key].Null() {
								current = v.Head.Row[key].Int
							}
							addSum(v, key, current, -oldValue.Int)
						}
					}
					if newMatch {
						newValue := newRow[column]
						if !newValue.Null() {
							v.Head.NonNull[key]++
							current := int64(0)
							if !v.Head.Row[key].Null() {
								current = v.Head.Row[key].Int
							}
							addSum(v, key, current, newValue.Int)
						}
					}
					if v.Head.NonNull[key] == 0 && !overflowColumn(v, key) {
						v.Head.Row[key] = datum.Value{Kind: "null"}
					}
				}
			}
		}
		// The head is always rebound to the exact source commit, whether or
		// not the aggregate values changed; retained history only stores a
		// new point when the values actually changed, and that point must be
		// stamped with the new source sequence.
		v.Head.Sequence = seq
		if changed && v.Def.History == HistoryRetained {
			v.Chain = append(v.Chain, clonePoint(v.Head))
		}
	}
	return nil
}

// Advance rebinds every eager view head to a source sequence that carried no
// row changes (for example a vacuum record), so an eager head can never lag
// the source head. History is unchanged because the values did not change.
func (m *Manager) Advance(seq uint64) {
	for _, v := range m.Views {
		if v.Def.Refresh == RefreshOnCommit {
			v.Head.Sequence = seq
		}
	}
}

func overflowColumn(v *View, key string) bool {
	for _, col := range v.OverflowColumns {
		if col == key {
			return true
		}
	}
	return false
}

func clonePoint(p Point) Point {
	return Point{Sequence: p.Sequence, Row: datum.Clone(p.Row), NonNull: cloneCounts(p.NonNull)}
}

func cloneCounts(in map[string]int64) map[string]int64 {
	if in == nil {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, n := range in {
		out[k] = n
	}
	return out
}

// Prune drops retained history below the vacuum horizon while keeping the
// last point at or below the horizon for interval reads.
func (m *Manager) Prune(horizon uint64) {
	for _, v := range m.Views {
		if v.Def.History != HistoryRetained || len(v.Chain) == 0 {
			continue
		}
		keep := 0
		for keep+1 < len(v.Chain) && v.Chain[keep+1].Sequence <= horizon {
			keep++
		}
		if keep > 0 {
			v.Chain = append([]Point(nil), v.Chain[keep:]...)
		}
		if len(v.Chain) > 0 && v.Chain[0].Sequence > v.Watermark {
			v.Watermark = v.Chain[0].Sequence
		}
		if horizon > v.Watermark {
			v.Watermark = horizon
		}
	}
}

// ReadResult explains one derived answer and its exact source position.
type ReadResult struct {
	Definition     Definition
	Point          Point
	Status         string
	Overflow       []string
	SourceSequence uint64
	// CaughtUp is true for lazy views after this read recomputed the head.
	CaughtUp bool
	// Materialized reports whether the answer came from maintained state
	// instead of a full source recomputation.
	Materialized bool
}

// ReadAt serves a view bound to snapshot at. Lazy views recompute from the
// source store; eager retained views read the maintained chain.
func (m *Manager) ReadAt(name string, at, currentSeq uint64, c *catalog.Catalog, s *versions.Store) (ReadResult, error) {
	v, err := m.view(name)
	if err != nil {
		return ReadResult{}, err
	}
	if at < v.Def.Created-1 {
		return ReadResult{}, fmt.Errorf("%w: view did not exist at sequence %d", ErrSnapshotGone, at)
	}
	if at > currentSeq {
		return ReadResult{}, fmt.Errorf("snapshot %d ahead of source head %d", at, currentSeq)
	}
	status := StatusActive
	if v.Overflow {
		status = StatusOverflow
	}
	if v.Def.Refresh == RefreshOnDemand {
		if v.Def.History == HistoryHeadOnly && at != currentSeq {
			return ReadResult{}, fmt.Errorf("%w: head-only view answers only the current source sequence %d", ErrSnapshotGone, currentSeq)
		}
		point, overflow, overflowColumns, err := compute(v.Def, at, c, s)
		if err != nil {
			return ReadResult{}, err
		}
		caughtUp := at == currentSeq
		if caughtUp {
			v.Head = point
			v.Overflow = overflow
			v.OverflowColumns = overflowColumns
		}
		resultStatus := StatusActive
		if overflow {
			resultStatus = StatusOverflow
		}
		return ReadResult{Definition: v.Def, Point: point, Status: resultStatus, Overflow: overflowColumns, SourceSequence: at, CaughtUp: caughtUp, Materialized: false}, nil
	}
	if v.Def.History == HistoryHeadOnly {
		if at < v.Head.Sequence {
			return ReadResult{}, fmt.Errorf("%w: head-only view retained only sequence %d", ErrSnapshotGone, v.Head.Sequence)
		}
		return ReadResult{Definition: v.Def, Point: clonePoint(v.Head), Status: status, Overflow: append([]string(nil), v.OverflowColumns...), SourceSequence: v.Head.Sequence, Materialized: true}, nil
	}
	// Retained history: enforce the pruned lower bound, then either return
	// the maintained head (for snapshots at/after it) or the greatest chain
	// point whose sequence is at or below the requested snapshot.
	if at < v.Watermark {
		return ReadResult{}, fmt.Errorf("%w: sequence %d reclaimed by watermark %d", ErrSnapshotGone, at, v.Watermark)
	}
	if at >= v.Head.Sequence {
		return ReadResult{Definition: v.Def, Point: clonePoint(v.Head), Status: status, Overflow: append([]string(nil), v.OverflowColumns...), SourceSequence: v.Head.Sequence, Materialized: true}, nil
	}
	idx := sort.Search(len(v.Chain), func(i int) bool { return v.Chain[i].Sequence > at })
	if idx == 0 {
		return ReadResult{}, fmt.Errorf("%w: sequence %d reclaimed", ErrSnapshotGone, at)
	}
	selected := v.Chain[idx-1]
	return ReadResult{Definition: v.Def, Point: clonePoint(selected), Status: status, Overflow: append([]string(nil), v.OverflowColumns...), SourceSequence: selected.Sequence, Materialized: true}, nil
}

// Verify recomputes every view from source state and fails closed on any
// mismatch, so recovery never trusts unexplained derived state. Lazy views are
// caught up as part of verification and never require a manual refresh.
func (m *Manager) Verify(c *catalog.Catalog, s *versions.Store, currentSeq uint64) error {
	names := make([]string, 0, len(m.Views))
	for name := range m.Views {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		v := m.Views[name]
		if v.Def.Refresh == RefreshOnCommit {
			point, overflow, overflowColumns, err := compute(v.Def, currentSeq, c, s)
			if err != nil {
				return fmt.Errorf("view %s: %w", name, err)
			}
			if v.Head.Sequence != currentSeq {
				return fmt.Errorf("view %s head %d trails source %d", name, v.Head.Sequence, currentSeq)
			}
			if v.Overflow != overflow || !sameStrings(v.OverflowColumns, overflowColumns) {
				return fmt.Errorf("view %s overflow state diverged from source", name)
			}
			if !overflow && !pointsEqual(v.Head, point) {
				return fmt.Errorf("view %s derived row diverged from source at %d", name, v.Head.Sequence)
			}
			if v.Def.History == HistoryRetained {
				// The chain stores only value-changing points, so its last
				// entry may carry an older sequence than the head; compare the
				// head values against the chain point valid at the head.
				idx := sort.Search(len(v.Chain), func(i int) bool { return v.Chain[i].Sequence > v.Head.Sequence })
				if idx == 0 || !rowsEqual(v.Chain[idx-1].Row, v.Head.Row) {
					return fmt.Errorf("view %s history chain diverged from head", name)
				}
			}
			continue
		}
		// Lazy view: catch the head up deterministically.
		head, headOverflow, headColumns, err := compute(v.Def, currentSeq, c, s)
		if err != nil {
			return fmt.Errorf("view %s: %w", name, err)
		}
		v.Head = head
		v.Overflow = headOverflow
		v.OverflowColumns = headColumns
	}
	return nil
}

func pointsEqual(a, b Point) bool {
	if a.Sequence != b.Sequence {
		return false
	}
	return rowsEqual(a.Row, b.Row)
}

func rowsEqual(a, b datum.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for k, value := range a {
		other, ok := b[k]
		if !ok {
			return false
		}
		if value.Kind != other.Kind || value.Int != other.Int || value.Text != other.Text || value.Bool != other.Bool {
			return false
		}
	}
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Clone returns an independent deep copy for checkpoint images.
func (m *Manager) Clone() *Manager {
	out := NewManager()
	for name, v := range m.Views {
		copyView := &View{
			Def:             v.Def,
			Head:            clonePoint(v.Head),
			Watermark:       v.Watermark,
			Overflow:        v.Overflow,
			OverflowColumns: append([]string(nil), v.OverflowColumns...),
		}
		if v.Chain != nil {
			copyView.Chain = make([]Point, len(v.Chain))
			for i, p := range v.Chain {
				copyView.Chain[i] = clonePoint(p)
			}
		}
		out.Views[name] = copyView
	}
	return out
}
