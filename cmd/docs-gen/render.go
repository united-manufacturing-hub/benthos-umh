// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

//go:embed templates/*.md.tmpl
var templateFS embed.FS

var templates = template.Must(template.New("").Funcs(template.FuncMap{
	"title": func(s string) string {
		if s == "" {
			return s
		}
		return strings.ToUpper(s[:1]) + s[1:]
	},
}).ParseFS(templateFS, "templates/*.md.tmpl"))

// section is one component type that gets generated pages.
type section struct {
	TypeDir  string // key in the schema and path segment upstream: inputs
	DocsDir  string // directory under docs/: input
	Singular string // input
}

var sections = []section{
	{TypeDir: "inputs", DocsDir: "input", Singular: "input"},
	{TypeDir: "outputs", DocsDir: "output", Singular: "output"},
}

func (s section) Plural() string { return s.TypeDir }

// pageDir is where generated pages of this section live, relative to docs/.
func (s section) pageDir() string { return s.DocsDir + "/connect" }

type pageView struct {
	section
	Name         string
	Title        string
	Version      string
	Summary      string
	StatusNote   string
	Overlay      string
	CommonYAML   string
	AdvancedYAML string
	Description  string
	Fields       []fieldView
	Examples     []componentExample
	Footnotes    string
	UpstreamURL  string
}

type fieldView struct {
	Path         string
	Description  string
	TypeLabel    string
	Default      string
	Required     bool
	Advanced     bool
	Secret       bool
	Interpolated bool
	Bloblang     bool
	Options      []optionView
	ExamplesYAML string
}

type optionView struct {
	Value       string
	Description string
}

// linkRewriter points links to upstream component pages at the generated
// page of that component when one exists.
type linkRewriter struct {
	from       section
	documented map[string]bool // "inputs/mqtt"
}

var componentLinkRe = regexp.MustCompile(
	regexp.QuoteMeta(upstreamDocsURL) + `/components/(inputs|outputs)/([a-z0-9_]+)/(#[\w.-]*)?`)

func (r linkRewriter) rewrite(md string) string {
	return componentLinkRe.ReplaceAllStringFunc(md, func(link string) string {
		m := componentLinkRe.FindStringSubmatch(link)
		typeDir, name := m[1], m[2]
		var target section
		for _, s := range sections {
			if s.TypeDir == typeDir {
				target = s
			}
		}
		file := name + ".md"
		switch {
		case name == "about":
			file = "README.md"
		case !r.documented[typeDir+"/"+name]:
			return link
		}
		// Anchors are dropped: GitBook derives its own heading anchors, so
		// upstream ones such as #tls-enabled would not resolve.
		if target.DocsDir == r.from.DocsDir {
			return file
		}
		return "../../" + target.pageDir() + "/" + file
	})
}

// renderPage renders the generated page of one component.
func renderPage(s section, c component, e entry, overlay, version string, rw linkRewriter) ([]byte, error) {
	hidden := map[string]bool{}
	for _, h := range e.Hide {
		hidden[h] = true
	}
	if missing := missingPaths(c.Config.Children, e.Hide); len(missing) > 0 {
		return nil, fmt.Errorf("%s %s: hide lists fields that do not exist: %s", s.Singular, c.Name, strings.Join(missing, ", "))
	}

	common, err := configYAML(s, c, hidden, false)
	if err != nil {
		return nil, err
	}
	advanced, err := configYAML(s, c, hidden, true)
	if err != nil {
		return nil, err
	}
	if advanced == common {
		advanced = ""
	}

	fields, err := fieldViews(c.Config.Children, "", hidden, rw)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", s.Singular, c.Name, err)
	}

	examples := make([]componentExample, 0, len(c.Examples))
	for _, ex := range c.Examples {
		examples = append(examples, componentExample{
			Title:   ex.Title,
			Summary: rw.rewrite(strings.TrimSpace(ex.Summary)),
			Config:  strings.TrimLeft(ex.Config, "\n"),
		})
	}

	description := rw.rewrite(strings.TrimSpace(c.Description))
	if description != "" && !strings.HasPrefix(description, "#") {
		description = "## Description\n\n" + description
	}

	view := pageView{
		section:      s,
		Name:         c.Name,
		Title:        e.Title,
		Version:      version,
		Summary:      rw.rewrite(strings.TrimSpace(c.Summary)),
		StatusNote:   statusNote(c.Status),
		Overlay:      strings.TrimSpace(overlay),
		CommonYAML:   common,
		AdvancedYAML: advanced,
		Description:  description,
		Fields:       fields,
		Examples:     examples,
		Footnotes:    rw.rewrite(strings.TrimSpace(c.Footnotes)),
		UpstreamURL:  fmt.Sprintf("%s/components/%s/%s/", upstreamDocsURL, s.TypeDir, c.Name),
	}
	return execute("page.md.tmpl", view)
}

type catalogView struct {
	section
	Version    string
	Documented []catalogRow
	Others     []catalogRow
}

type catalogRow struct {
	Title   string
	Name    string
	Summary string
	Status  string
	URL     string
}

// renderCatalog renders the index page of a section: the documented
// components first, then every other upstream component the binary ships.
func renderCatalog(s section, all []component, entries []entry, version string) ([]byte, error) {
	byName := map[string]component{}
	for _, c := range all {
		byName[c.Name] = c
	}
	view := catalogView{section: s, Version: version}
	documented := map[string]bool{}
	for _, e := range entries {
		c := byName[e.Name]
		documented[e.Name] = true
		view.Documented = append(view.Documented, catalogRow{
			Title: e.Title, Name: c.Name, Summary: tableCell(c.Summary), URL: c.Name + ".md",
		})
	}
	for _, c := range all {
		if documented[c.Name] || c.Status == "deprecated" {
			continue
		}
		status := ""
		if c.Status != "stable" {
			status = c.Status
		}
		view.Others = append(view.Others, catalogRow{
			Name:    c.Name,
			Summary: tableCell(c.Summary),
			Status:  status,
			URL:     fmt.Sprintf("%s/components/%s/%s/", upstreamDocsURL, s.TypeDir, c.Name),
		})
	}
	return execute("catalog.md.tmpl", view)
}

func execute(name string, data any) ([]byte, error) {
	var buf bytes.Buffer
	if err := templates.ExecuteTemplate(&buf, name, data); err != nil {
		return nil, fmt.Errorf("executing %s: %w", name, err)
	}
	return tidy(buf.Bytes()), nil
}

var blankLinesRe = regexp.MustCompile(`\n{3,}`)

// tidy collapses the blank lines that optional template sections leave.
func tidy(b []byte) []byte {
	return append(bytes.TrimSpace(blankLinesRe.ReplaceAll(b, []byte("\n\n"))), '\n')
}

func tableCell(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	return strings.ReplaceAll(s, "|", `\|`)
}

func statusNote(status string) string {
	switch status {
	case "beta":
		return "Beta: this component can still change in a future Redpanda Connect release."
	case "experimental":
		return "Experimental: this component can change or be removed in a future Redpanda Connect release."
	case "deprecated":
		return "Deprecated: Redpanda Connect will remove this component. Do not use it in new flows."
	}
	return ""
}

func childPath(prefix string, f field) string {
	p := prefix + f.Name
	if f.Kind == "array" {
		return p + "[]."
	}
	return p + "."
}

// missingPaths returns the entries of paths that name no field.
func missingPaths(fields []field, paths []string) []string {
	known := map[string]bool{}
	var walk func([]field, string)
	walk = func(fs []field, prefix string) {
		for _, f := range fs {
			known[prefix+f.Name] = true
			walk(f.Children, childPath(prefix, f))
		}
	}
	walk(fields, "")
	var missing []string
	for _, p := range paths {
		if !known[p] {
			missing = append(missing, p)
		}
	}
	return missing
}

func fieldViews(fields []field, prefix string, hidden map[string]bool, rw linkRewriter) ([]fieldView, error) {
	var out []fieldView
	for _, f := range fields {
		path := prefix + f.Name
		if f.IsDeprecated || hidden[path] {
			continue
		}
		v := fieldView{
			Path:         path,
			Description:  rw.rewrite(strings.TrimSpace(f.Description)),
			TypeLabel:    typeLabel(f),
			Required:     !f.hasDefault() && !f.IsOptional && len(f.Children) == 0,
			Advanced:     f.IsAdvanced,
			Secret:       f.IsSecret,
			Interpolated: f.Interpolated,
			Bloblang:     f.Bloblang,
		}
		if f.hasDefault() {
			v.Default = string(f.Default)
		}
		for _, o := range f.Options {
			v.Options = append(v.Options, optionView{Value: o})
		}
		for _, o := range f.AnnotatedOptions {
			v.Options = append(v.Options, optionView{Value: o[0], Description: tableCell(rw.rewrite(o[1]))})
		}
		if len(f.Examples) > 0 {
			ex, err := examplesYAML(f)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", path, err)
			}
			v.ExamplesYAML = ex
		}
		out = append(out, v)

		children, err := fieldViews(f.Children, childPath(prefix, f), hidden, rw)
		if err != nil {
			return nil, err
		}
		out = append(out, children...)
	}
	return out, nil
}

func typeLabel(f field) string {
	switch f.Kind {
	case "array":
		return "array of " + f.Type
	case "2darray":
		return "array of arrays of " + f.Type
	case "map":
		return "map of " + f.Type
	}
	return f.Type
}

func examplesYAML(f field) (string, error) {
	parts := make([]string, 0, len(f.Examples))
	for _, ex := range f.Examples {
		b, err := marshalYAML(map[string]any{f.Name: ex})
		if err != nil {
			return "", err
		}
		parts = append(parts, b)
	}
	return strings.Join(parts, "\n"), nil
}

// configYAML renders a config snippet for the component. The common snippet
// leaves out advanced fields; neither shows deprecated or hidden ones.
func configYAML(s section, c component, hidden map[string]bool, advanced bool) (string, error) {
	body, err := fieldsNode(c.Config.Children, "", hidden, advanced)
	if err != nil {
		return "", err
	}
	root := mapping(
		scalar(s.Singular), mapping(
			scalar("label"), &yaml.Node{Kind: yaml.ScalarNode, Value: "", Style: yaml.DoubleQuotedStyle},
			scalar(c.Name), body,
		),
	)
	return marshalYAML(root)
}

func fieldsNode(fields []field, prefix string, hidden map[string]bool, advanced bool) (*yaml.Node, error) {
	m := mapping()
	for _, f := range fields {
		path := prefix + f.Name
		if f.IsDeprecated || hidden[path] || (f.IsAdvanced && !advanced) {
			continue
		}
		v, err := valueNode(f, path, hidden, advanced)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", path, err)
		}
		m.Content = append(m.Content, scalar(f.Name), v)
	}
	if len(m.Content) == 0 {
		m.Style = yaml.FlowStyle
	}
	return m, nil
}

func valueNode(f field, path string, hidden map[string]bool, advanced bool) (*yaml.Node, error) {
	if f.Kind == "scalar" && len(f.Children) > 0 {
		return fieldsNode(f.Children, path+".", hidden, advanced)
	}
	if f.hasDefault() {
		var v any
		if err := json.Unmarshal(f.Default, &v); err != nil {
			return nil, err
		}
		n := &yaml.Node{}
		if err := n.Encode(v); err != nil {
			return nil, err
		}
		if n.Kind == yaml.SequenceNode || n.Kind == yaml.MappingNode {
			n.Style = yaml.FlowStyle
		}
		return n, nil
	}
	var n *yaml.Node
	switch f.Kind {
	case "array", "2darray":
		n = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Style: yaml.FlowStyle}
	case "map":
		n = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Style: yaml.FlowStyle}
	default:
		n = &yaml.Node{Kind: yaml.ScalarNode, Value: "", Style: yaml.DoubleQuotedStyle}
	}
	n.LineComment = "No default (required)"
	if f.IsOptional {
		n.LineComment = "No default (optional)"
	}
	return n, nil
}

func scalar(v string) *yaml.Node { return &yaml.Node{Kind: yaml.ScalarNode, Value: v} }

func mapping(content ...*yaml.Node) *yaml.Node {
	return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: content}
}

func marshalYAML(v any) (string, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	if err := enc.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}
