package saci

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

// tagKey names a field's wire column. Without it the column is the lower
// snake case of the Go field name, which is what a struct written to Go's own
// naming conventions already spells.
const tagKey = "saci"

// schemaVersion is the `__saci_schema_version` every component this SDK writes
// declares. The host defaults an unparseable version to 1 and registers its own
// components at 1, so a processor that reported anything else would fail the
// load-time compatibility check.
const schemaVersion uint32 = 1

// component is a row type's wire identity: the component name, its schema
// version, and its fields in declaration order.
//
// Field order is the schema. It fixes the buffer walk on both sides of the
// boundary and the fingerprint the host gates on, so it is taken from the
// struct rather than sorted.
type component struct {
	name    string
	version uint32
	fields  []arrowipc.SchemaField
}

// schemas caches one derived component per row type.
//
// Reflection over a struct costs a scan of every field and a tag parse per
// field, and a processor asks for the same row type on every batch. The
// [sync.Once] guards the derivation rather than the map because two goroutines
// finding a fresh entry must not both panic on the same bad field.
var schemas sync.Map // reflect.Type -> *schemaEntry

type schemaEntry struct {
	once sync.Once
	spec component
}

// componentOf derives a row type's component, once per type.
func componentOf(t reflect.Type) component {
	entry, _ := schemas.LoadOrStore(t, &schemaEntry{})
	e := entry.(*schemaEntry)
	e.once.Do(func() { e.spec = deriveComponent(t) })
	return e.spec
}

// deriveComponent reads a row type's schema off its fields.
//
// Every refusal here panics. These are authoring mistakes, not input: a field
// this SDK cannot map to an Arrow type is wrong for every batch, and reporting
// it as a run error would hide it behind whatever data happened to arrive
// first. [Transform] derives at construction time, so a stage that declares one
// fails when the component is instantiated rather than mid-batch.
func deriveComponent(t reflect.Type) component {
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("saci: row type %s is a %s, want a struct", t, t.Kind()))
	}
	if t.Name() == "" {
		panic("saci: row type is anonymous, and a row type's name is its component name")
	}
	if t.NumField() == 0 {
		panic(fmt.Sprintf("saci: row type %s declares no fields", t.Name()))
	}

	fields := make([]arrowipc.SchemaField, t.NumField())
	for i := range fields {
		f := t.Field(i)
		switch {
		case f.Anonymous:
			panic(fmt.Sprintf("saci: %s embeds %s, and a row type's fields are its columns in declaration order", t.Name(), f.Name))
		case !f.IsExported():
			panic(fmt.Sprintf("saci: %s.%s is unexported, so reflection cannot decode a column into it", t.Name(), f.Name))
		}

		name := wireName(t.Name(), f)
		for _, prior := range fields[:i] {
			if prior.Name == name {
				panic(fmt.Sprintf("saci: %s declares column %q twice", t.Name(), name))
			}
		}
		fields[i] = arrowipc.SchemaField{Name: name, Type: columnType(t.Name(), f)}
	}
	return component{name: t.Name(), version: schemaVersion, fields: fields}
}

// wireName is the column a struct field carries.
func wireName(owner string, f reflect.StructField) string {
	tag, tagged := f.Tag.Lookup(tagKey)
	if !tagged {
		return snakeCase(f.Name)
	}
	if strings.TrimSpace(tag) == "" {
		panic(fmt.Sprintf("saci: %s.%s has an empty %s tag, so it names no column", owner, f.Name, tagKey))
	}
	return tag
}

// columnType maps a field's Go kind to its Arrow type.
//
// The four kinds are the wire format's four types. A narrower integer or float
// would have to widen on the way in and could not round-trip on the way out, and
// a slice, pointer or nested struct has no column layout at all, so all of them
// are refused by name rather than coerced.
func columnType(owner string, f reflect.StructField) arrowipc.ColumnType {
	switch f.Type.Kind() {
	case reflect.Int64:
		return arrowipc.TypeInt64
	case reflect.Float64:
		return arrowipc.TypeFloat64
	case reflect.Bool:
		return arrowipc.TypeBool
	case reflect.String:
		return arrowipc.TypeUtf8
	default:
		panic(fmt.Sprintf(
			"saci: %s.%s is a %s, and a row field must be int64, float64, bool or string",
			owner, f.Name, f.Type.Kind(),
		))
	}
}

// snakeCase turns a Go field name into its wire column.
//
// A boundary opens where a lower case rune meets an upper case one, and at the
// last rune of an upper case run that starts a new word, so `UsdAmountDisplay`
// becomes `usd_amount_display`, `ID` becomes `id` and `USDAmount` becomes
// `usd_amount`.
func snakeCase(name string) string {
	runes := []rune(name)
	out := make([]byte, 0, len(name)+4)
	for i, r := range runes {
		if !unicode.IsUpper(r) {
			out = utf8.AppendRune(out, r)
			continue
		}
		if i > 0 && (!unicode.IsUpper(runes[i-1]) || (i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
			out = append(out, '_')
		}
		out = utf8.AppendRune(out, unicode.ToLower(r))
	}
	return string(out)
}

// fingerprint hashes a component list into `pipeline-descriptor.schema-fingerprint`.
//
// FNV-1a over the component name, the schema version as four little-endian
// bytes, then every field name in schema declaration order, for each component
// in the order given. Names and versions only: adding a field changes the value,
// changing a field's type does not. The host computes the same value from its
// own registration and refuses a processor that disagrees, so the byte order
// here is the contract, not an implementation choice.
func fingerprint(components []component) string {
	const (
		offsetBasis uint32 = 2166136261
		prime       uint32 = 16777619
	)

	hash := offsetBasis
	mix := func(b byte) { hash = (hash ^ uint32(b)) * prime }
	mixString := func(s string) {
		for i := range len(s) {
			mix(s[i])
		}
	}

	for _, c := range components {
		mixString(c.name)
		var version [4]byte
		binary.LittleEndian.PutUint32(version[:], c.version)
		for _, b := range version {
			mix(b)
		}
		for _, f := range c.fields {
			mixString(f.Name)
		}
	}
	return fmt.Sprintf("%08x", hash)
}
