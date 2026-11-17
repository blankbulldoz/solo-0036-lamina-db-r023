package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"lamina/internal/catalog"
	"lamina/internal/materialize"
	"lamina/internal/versions"
	"os"
)

const MaxImage = 128 << 20

type Image struct {
	Format   int                  `json:"format"`
	Sequence uint64               `json:"sequence"`
	Horizon  uint64               `json:"horizon"`
	Catalog  *catalog.Catalog     `json:"catalog"`
	Store    *versions.Store      `json:"store"`
	Views    *materialize.Manager `json:"views,omitempty"`
}
type Envelope struct {
	Format  int             `json:"format"`
	Digest  string          `json:"sha256"`
	Payload json.RawMessage `json:"payload"`
}

func Encode(image Image) ([]byte, error) {
	image.Format = 1
	b, err := json.Marshal(image)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	return json.Marshal(Envelope{Format: 1, Digest: hex.EncodeToString(h[:]), Payload: b})
}
func Decode(b []byte) (Image, error) {
	var image Image
	var env Envelope
	if len(b) > MaxImage {
		return image, fmt.Errorf("image exceeds limit")
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return image, err
	}
	h := sha256.Sum256(env.Payload)
	if env.Format != 1 || hex.EncodeToString(h[:]) != env.Digest {
		return image, fmt.Errorf("image format or digest mismatch")
	}
	if err := json.Unmarshal(env.Payload, &image); err != nil {
		return image, err
	}
	if err := Validate(image); err != nil {
		return image, err
	}
	return image, nil
}
func Read(path string) (Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return Image{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxImage+1))
	if err != nil {
		return Image{}, err
	}
	return Decode(b)
}
func Validate(image Image) error {
	if image.Format != 1 || image.Horizon > image.Sequence || image.Catalog == nil || image.Store == nil || image.Catalog.Relations == nil || image.Catalog.Indexes == nil || image.Store.Tables == nil {
		return fmt.Errorf("invalid image header")
	}
	for name, r := range image.Catalog.Relations {
		if name != r.Name || r.Created == 0 || r.Created > image.Sequence {
			return fmt.Errorf("invalid catalog sequence")
		}
		if err := r.Validate(); err != nil {
			return err
		}
	}
	for name, definition := range image.Catalog.Indexes {
		relation, ok := image.Catalog.Relations[definition.Relation]
		if !ok || name != definition.Name || !catalog.ValidName(name) || definition.Created < relation.Created || definition.Created > image.Sequence {
			return fmt.Errorf("invalid index metadata")
		}
		if _, ok := relation.Column(definition.Column); !ok {
			return fmt.Errorf("invalid index column")
		}
	}
	for name, rows := range image.Store.Tables {
		r, ok := image.Catalog.Relations[name]
		if !ok {
			return fmt.Errorf("image references unknown relation")
		}
		for key, chain := range rows {
			var last uint64
			if len(chain) == 0 {
				return fmt.Errorf("empty version chain")
			}
			for _, v := range chain {
				if v.Sequence <= last || v.Sequence > image.Sequence || v.Sequence <= r.Created {
					return fmt.Errorf("invalid version order")
				}
				last = v.Sequence
				if !v.Deleted {
					row, err := r.Normalize(v.Row)
					if err != nil || r.Key(row) != key {
						return fmt.Errorf("invalid stored tuple")
					}
				}
			}
		}
	}
	for _, definition := range image.Catalog.Indexes {
		if !definition.Unique {
			continue
		}
		seen := map[string]bool{}
		for _, chain := range image.Store.Tables[definition.Relation] {
			version, exists := versions.Visible(chain, image.Sequence)
			if !exists {
				continue
			}
			value := version.Row[definition.Column]
			if value.Null() {
				continue
			}
			if seen[value.Key()] {
				return fmt.Errorf("image violates unique index %s", definition.Name)
			}
			seen[value.Key()] = true
		}
	}
	if image.Views != nil {
		for name, view := range image.Views.Views {
			if view == nil || name != view.Def.Name || !catalog.ValidName(name) {
				return fmt.Errorf("invalid view metadata")
			}
			if view.Def.Created == 0 || view.Def.Created > image.Sequence {
				return fmt.Errorf("invalid view creation sequence")
			}
			if _, ok := image.Catalog.Relations[view.Def.Relation]; !ok {
				return fmt.Errorf("view references unknown relation")
			}
			if err := materialize.ValidateDefinition(image.Catalog, view.Def); err != nil {
				return fmt.Errorf("invalid view definition: %w", err)
			}
			if view.Head.Sequence > image.Sequence {
				return fmt.Errorf("view head ahead of image sequence")
			}
			if view.Watermark < image.Horizon {
				return fmt.Errorf("view watermark below image horizon")
			}
			last := uint64(0)
			for _, point := range view.Chain {
				if point.Sequence <= last {
					return fmt.Errorf("view history not ordered by sequence")
				}
				last = point.Sequence
			}
		}
	}

	return nil
}
