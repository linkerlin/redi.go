package redi

import (
	"context"
	"fmt"
	"strings"
)

// IndexFieldType is a RediSearch schema field type.
type IndexFieldType string

const (
	IndexText    IndexFieldType = "TEXT"
	IndexNumeric IndexFieldType = "NUMERIC"
	IndexTag     IndexFieldType = "TAG"
	IndexGeo     IndexFieldType = "GEO"
)

// IndexField describes one SCHEMA field.
type IndexField struct {
	Name  string
	Type  IndexFieldType
	Alias string
}

// IndexOptions configures FT.CREATE.
type IndexOptions struct {
	On       string // HASH or JSON (default HASH)
	Prefixes []string
}

// SearchOptions configures FT.SEARCH.
type SearchOptions struct {
	LimitOffset int
	LimitCount  int
	Return      []string
	WithScores  bool
	NoContent   bool
}

// SearchDocument is one hit.
type SearchDocument struct {
	ID     string
	Score  float64
	Fields map[string]string
}

// SearchResult is FT.SEARCH output.
type SearchResult struct {
	Total     int64
	Documents []SearchDocument
}

// RSearch is a RediSearch facade (create / search / drop subset).
type RSearch struct{ c *Client }

func newRSearch(c *Client) *RSearch { return &RSearch{c: c} }

// CreateIndex creates an index.
func (s *RSearch) CreateIndex(ctx context.Context, name string, opts IndexOptions, fields ...IndexField) error {
	if len(fields) == 0 {
		return fmt.Errorf("redi: search index requires fields")
	}
	on := opts.On
	if on == "" {
		on = "HASH"
	}
	args := []any{"FT.CREATE", name, "ON", on}
	if len(opts.Prefixes) > 0 {
		args = append(args, "PREFIX", len(opts.Prefixes))
		for _, p := range opts.Prefixes {
			args = append(args, p)
		}
	}
	args = append(args, "SCHEMA")
	for _, f := range fields {
		args = append(args, f.Name)
		if f.Alias != "" {
			args = append(args, "AS", f.Alias)
		}
		args = append(args, string(f.Type))
	}
	return s.c.rc.Do(ctx, args...).Err()
}

// DropIndex drops an index; deleteDocuments maps to DD.
func (s *RSearch) DropIndex(ctx context.Context, name string, deleteDocuments bool) error {
	args := []any{"FT.DROPINDEX", name}
	if deleteDocuments {
		args = append(args, "DD")
	}
	return s.c.rc.Do(ctx, args...).Err()
}

// Search runs FT.SEARCH.
func (s *RSearch) Search(ctx context.Context, name, query string, opts SearchOptions) (SearchResult, error) {
	args := []any{"FT.SEARCH", name, query}
	if opts.NoContent {
		args = append(args, "NOCONTENT")
	}
	if opts.WithScores {
		args = append(args, "WITHSCORES")
	}
	if len(opts.Return) > 0 {
		args = append(args, "RETURN", len(opts.Return))
		for _, f := range opts.Return {
			args = append(args, f)
		}
	}
	if opts.LimitCount > 0 || opts.LimitOffset > 0 {
		args = append(args, "LIMIT", opts.LimitOffset, opts.LimitCount)
	}
	res, err := s.c.rc.Do(ctx, args...).Result()
	if err != nil {
		return SearchResult{}, err
	}
	switch typed := res.(type) {
	case []any:
		return parseSearchResult(typed, opts.WithScores, opts.NoContent), nil
	case map[any]any:
		return parseSearchMap(typed), nil
	case map[string]any:
		converted := make(map[any]any, len(typed))
		for k, v := range typed {
			converted[k] = v
		}
		return parseSearchMap(converted), nil
	default:
		return SearchResult{}, fmt.Errorf("redi: unexpected FT.SEARCH reply %T", res)
	}
}

func parseSearchMap(m map[any]any) SearchResult {
	out := SearchResult{}
	if t, ok := m["total_results"]; ok {
		out.Total = firstInt64(t)
	}
	if results, ok := m["results"].([]any); ok {
		for _, item := range results {
			doc := SearchDocument{Fields: map[string]string{}}
			rm, ok := item.(map[any]any)
			if !ok {
				if rm2, ok2 := item.(map[string]any); ok2 {
					rm = make(map[any]any, len(rm2))
					for k, v := range rm2 {
						rm[k] = v
					}
				} else {
					continue
				}
			}
			doc.ID = fmt.Sprint(rm["id"])
			if attrs, ok := rm["extra_attributes"].(map[any]any); ok {
				for k, v := range attrs {
					doc.Fields[fmt.Sprint(k)] = fmt.Sprint(v)
				}
			}
			if attrs, ok := rm["extra_attributes"].(map[string]any); ok {
				for k, v := range attrs {
					doc.Fields[k] = fmt.Sprint(v)
				}
			}
			out.Documents = append(out.Documents, doc)
		}
		if out.Total == 0 {
			out.Total = int64(len(out.Documents))
		}
	}
	return out
}

func parseSearchResult(res []any, withScores, noContent bool) SearchResult {
	out := SearchResult{}
	if len(res) == 0 {
		return out
	}
	switch n := res[0].(type) {
	case int64:
		out.Total = n
	case string:
		fmt.Sscan(n, &out.Total)
	}
	i := 1
	for i < len(res) {
		doc := SearchDocument{Fields: map[string]string{}}
		doc.ID = fmt.Sprint(res[i])
		i++
		if withScores && i < len(res) {
			fmt.Sscan(fmt.Sprint(res[i]), &doc.Score)
			i++
		}
		if !noContent && i < len(res) {
			if fields, ok := res[i].([]any); ok {
				for j := 0; j+1 < len(fields); j += 2 {
					doc.Fields[fmt.Sprint(fields[j])] = fmt.Sprint(fields[j+1])
				}
				i++
			}
		}
		out.Documents = append(out.Documents, doc)
	}
	return out
}

func skipSearchModule(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown command") && strings.Contains(s, "ft.")
}
