package transport

import (
	"lamina/internal/materialize"
	"net/http"
)

func (s *Server) listViews(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.ListViews()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"views": out})
}

func (s *Server) createView(w http.ResponseWriter, r *http.Request) {
	var in materialize.Definition
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.CreateView(in)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 201, out)
}

type viewReadRequest struct {
	At *uint64 `json:"at"`
}

func (s *Server) readView(w http.ResponseWriter, r *http.Request) {
	var in viewReadRequest
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.ReadView(r.PathValue("name"), in.At)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

func (s *Server) verifyView(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.VerifyView(r.PathValue("name"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
