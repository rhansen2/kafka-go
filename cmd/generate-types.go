package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"text/template"
	"unicode"

	"github.com/segmentio/kafka-go/protocol"
)

type VersionRange struct {
	Min int
	Max int
}

func (v VersionRange) ForEach(f func(int)) {
	for i := v.Min; i <= v.Max; i++ {
		f(i)
	}
}

func (v *VersionRange) Contains(version int) bool {
	if v == nil {
		return false
	}
	return version >= v.Min && version <= v.Max
}

func (v VersionRange) IsNone() bool {
	return v.Min == 0 && v.Max == 0
}

func (v VersionRange) HasMax() bool {
	return v.Max != math.MaxInt
}

func (v *VersionRange) UnmarshalJSON(data []byte) error {
	var vrString string
	if err := json.Unmarshal(data, &vrString); err != nil {
		return err
	}

	if vrString == "none" {
		return nil
	}

	parts := strings.Split(vrString, "-")
	hasPlus := strings.Contains(parts[0], "+")
	minStr := strings.Trim(parts[0], "+")
	min, err := strconv.ParseInt(minStr, 10, 32)
	if err != nil {
		return err
	}

	v.Min = int(min)
	v.Max = int(min)

	if hasPlus {
		v.Max = math.MaxInt
	}

	// if we have a second part we have a max value in the range.
	if len(parts) == 2 {
		max, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return err
		}
		v.Max = int(max)
	}

	return nil
}

type FieldType string

func (f FieldType) CanBeCompact() bool {
	return f == "string" || f == "bytes"
}

type Field struct {
	Name             string        `json:"name"`
	Type             FieldType     `json:"type"`
	Versions         VersionRange  `json:"versions"`
	EntityType       string        `json:"entityType"`
	Ignorable        bool          `json:"ignoreble"`
	About            string        `json:"about"`
	NullableVersions *VersionRange `json:"nullableVersions"`
	Fields           Fields        `json:"fields"`
	Tag              *int          `json:"tag,"`
	FlexibleVersions *VersionRange `json:"taggedVersions"`
	StructTag        Tags
}

func (f *Field) normalizeVersions(max int, flexibleRange VersionRange) {
	if f.FlexibleVersions != nil {
		f.FlexibleVersions.Max = max
	} else {
		copy := flexibleRange
		f.FlexibleVersions = &copy
	}
}

func (f *Field) GenerateTag(versions, flexibleRange VersionRange) {
	versions.ForEach(func(version int) {
		if !f.Versions.Contains(version) {
			return
		}
		tag := &Tag{
			StartVersion: version,
			EndVersion:   version,
			Tag:          f.Tag,
		}
		if flexibleRange.Contains(version) {
			tag.Compact = f.Type.CanBeCompact()
		}
		tag.Nullable = f.NullableVersions.Contains(version)
		f.StructTag.Append(tag)
	})
	f.Fields.GenerateTags(versions, flexibleRange)
}

func (f Field) NeedsType() bool {
	return len(f.Fields) > 0
}

type Tag struct {
	StartVersion int
	EndVersion   int
	Compact      bool
	Nullable     bool
	Tag          *int
}

func (t Tag) String() string {
	var s strings.Builder
	s.WriteString("min=")
	s.WriteString(strconv.Itoa(t.StartVersion))
	s.WriteString(",max=")
	s.WriteString(strconv.Itoa(t.EndVersion))
	if t.Compact {
		s.WriteString(",compact")
	}
	if t.Nullable {
		s.WriteString(",nullable")
	}
	if t.Tag != nil {
		if *t.Tag < 0 {
			s.WriteString(",tag")
		} else {
			s.WriteString(",tag=")
			s.WriteString(strconv.Itoa(*t.Tag))
		}
	}
	return s.String()
}

type Tags []*Tag

func (t Tags) String() string {
	var s strings.Builder
	s.WriteString("kafka:\"")
	for i, tag := range t {
		if i != 0 {
			s.WriteString("|")
		}
		fmt.Fprintf(&s, "%s", tag)
	}
	s.WriteString("\"")
	return s.String()
}

func (t *Tags) Append(tag *Tag) {
	if len(*t) == 0 {
		*t = append(*t, tag)
	}
	curr := (*t)[len(*t)-1]
	curr.EndVersion = tag.EndVersion
	if curr.Nullable != tag.Nullable {
		curr.EndVersion = tag.StartVersion - 1
		*t = append(*t, tag)
	}
	if curr.Compact != tag.Compact {
		curr.EndVersion = tag.StartVersion - 1
		*t = append(*t, tag)
	}
}

type Fields []Field

func (fs Fields) GenerateTags(versions, flexibleRange VersionRange) {
	if len(fs) == 0 {
		return
	}
	for i := range fs {
		// fmt.Println("Generating tag for ", fs[i])
		fs[i].GenerateTag(versions, flexibleRange)
	}
}

func (fs Fields) normalizeVersions(max int, flexibleRange VersionRange) {
	for i := range fs {
		fs[i].normalizeVersions(max, flexibleRange)
	}
}

type APIType struct {
	APIKey           protocol.ApiKey `json:"apiKey"`
	Type             string          `json:"type"`
	Name             string          `json:"name"`
	ValidVersions    VersionRange    `json:"validVersions"`
	FlexibleVersions VersionRange    `json:"flexibleVersions"`
	Fields           Fields          `json:"fields"`
}

func (a *APIType) UnmarshalJSON(data []byte) error {
	type apiType APIType
	var t apiType
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	*a = APIType(t)
	a.normalizeVerions()
	a.GenerateTags()

	return nil
}

func (a *APIType) normalizeVerions() {
	a.FlexibleVersions.Max = a.ValidVersions.Max
	a.Fields.normalizeVersions(a.ValidVersions.Max, a.FlexibleVersions)
}

func (a APIType) GenerateTags() {
	a.Fields.GenerateTags(a.ValidVersions, a.FlexibleVersions)
}

var helpers = template.FuncMap{
	"normalizeType": func(t string) string {
		if t == "bytes" {
			return "[]byte"
		}
		return t
	},
	"normalizeName": func(name string) string {
		name = strings.Title(name)
		name = strings.ReplaceAll(name, "Id", "ID")
		name = strings.ReplaceAll(name, "Ms", "MS")
		name = strings.TrimPrefix(name, "[]")
		return name
	},
	"backTick": func(s string) string {
		return fmt.Sprintf("`%s`", s)
	},
}

const requestTemplate = `
type {{ printf "%s" .Type | normalizeName }} struct {
{{ if .FlexibleVersions }}
        _ struct{} {{ printf "kafka:\"min=%d,max=%d,tag\"" .FlexibleVersions.Min .FlexibleVersions.Max | backTick}}
{{ end }}
{{- range .Fields }}
	{{ .Name | normalizeName }} {{ printf "%s" .Type | normalizeType }} {{ printf "%s" .StructTag | backTick }}
{{- end }}
}

{{ if or (eq "request" .Type) (eq "response" .Type )}}
func (r *{{ printf "%s" .Type | normalizeName }})  ApiKey() protocol.ApiKey { return protcol.{{ .APIKey }} }
{{ end }}
`

func main() {
	req := template.New("requests").Funcs(helpers)
	reqTemplate, err := req.Parse(requestTemplate)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	r := csReader{
		scanner: scanner,
	}

	dec := json.NewDecoder(&r)

	for {
		var t APIType

		if err := dec.Decode(&t); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			panic(err)
		}

		t.GenerateTags()

		for _, field := range t.Fields {
			if err := renderField(os.Stdout, field, reqTemplate); err != nil {
				panic(err)
			}
		}
		if err := reqTemplate.Execute(os.Stdout, t); err != nil {
			panic(err)
		}

	}
}

func renderField(w io.Writer, f Field, templates *template.Template) error {
	if !f.NeedsType() {
		return nil
	}
	for _, field := range f.Fields {
		if field.NeedsType() {
			if err := renderField(w, field, templates); err != nil {
				return err
			}
		}
	}

	return templates.Execute(w, f)
}

type csReader struct {
	scanner  *bufio.Scanner
	currLine []byte
	lineIdx  int
	err      error
}

func (cs *csReader) Read(b []byte) (n int, err error) {
	if cs.currLine != nil {
		return cs.read(b)
	}

	var hasScanned bool
	for cs.scanner.Scan() {
		hasScanned = true
		line := stripComment(cs.scanner.Text())
		if len(line) != 0 {
			cs.currLine = append(cs.currLine, []byte(line)...)
		}
	}
	err = cs.scanner.Err()
	if err != nil {
		return n, err
	}

	if hasScanned {
		cs.currLine = append(cs.currLine, '\n')
	}

	return cs.read(b)
}

func (cs *csReader) read(b []byte) (n int, err error) {
	if cs.currLine == nil {
		return 0, cs.err
	}
	n = copy(b, cs.currLine[cs.lineIdx:])
	cs.lineIdx += n
	if cs.lineIdx >= len(cs.currLine) {
		cs.currLine = nil
		cs.lineIdx = 0
		cs.err = io.EOF
	}
	return n, nil
}

var _ io.Reader = &csReader{}

func stripComment(line string) string {
	trimmed := strings.TrimLeftFunc(line, func(r rune) bool {
		return unicode.IsSpace(r)
	})

	if strings.HasPrefix(trimmed, "//") {
		return ""
	}
	return line
}
