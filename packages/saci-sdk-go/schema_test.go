package saci

import (
	"reflect"
	"strings"
	"testing"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

// Order is the polyglot chain's row type, and the cross-language contract: these
// twelve fields in this order are what every stage of the chain agrees on, and
// what the fingerprint below is computed from.
type Order struct {
	ID               int64   `saci:"id"`
	Region           string  `saci:"region"`
	Currency         string  `saci:"currency"`
	Amount           float64 `saci:"amount"`
	Valid            bool    `saci:"valid"`
	UsdAmount        float64 `saci:"usd_amount"`
	UsdAmountDisplay string  `saci:"usd_amount_display"`
	RiskScore        float64 `saci:"risk_score"`
	Flagged          bool    `saci:"flagged"`
	Fee              float64 `saci:"fee"`
	ReviewTier       int64   `saci:"review_tier"`
	Settlement       string  `saci:"settlement"`
}

func TestSnakeCase(t *testing.T) {
	cases := map[string]string{
		"ID":               "id",
		"Amount":           "amount",
		"ReviewTier":       "review_tier",
		"UsdAmountDisplay": "usd_amount_display",
		"USDAmount":        "usd_amount",
		"HTTPStatus":       "http_status",
		"Field2Name":       "field2_name",
		"A":                "a",
	}
	for in, want := range cases {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestDeriveComponentOrder pins the schema the reflection derives: the component
// name, the version, and every field's wire name and Arrow type in declaration
// order.
func TestDeriveComponentOrder(t *testing.T) {
	spec := componentOf(reflect.TypeFor[Order]())

	if spec.name != "Order" {
		t.Errorf("component name = %q, want Order", spec.name)
	}
	if spec.version != 1 {
		t.Errorf("schema version = %d, want 1", spec.version)
	}

	want := []arrowipc.SchemaField{
		{Name: "id", Type: arrowipc.TypeInt64},
		{Name: "region", Type: arrowipc.TypeUtf8},
		{Name: "currency", Type: arrowipc.TypeUtf8},
		{Name: "amount", Type: arrowipc.TypeFloat64},
		{Name: "valid", Type: arrowipc.TypeBool},
		{Name: "usd_amount", Type: arrowipc.TypeFloat64},
		{Name: "usd_amount_display", Type: arrowipc.TypeUtf8},
		{Name: "risk_score", Type: arrowipc.TypeFloat64},
		{Name: "flagged", Type: arrowipc.TypeBool},
		{Name: "fee", Type: arrowipc.TypeFloat64},
		{Name: "review_tier", Type: arrowipc.TypeInt64},
		{Name: "settlement", Type: arrowipc.TypeUtf8},
	}
	if len(spec.fields) != len(want) {
		t.Fatalf("derived %d fields, want %d", len(spec.fields), len(want))
	}
	for i, f := range want {
		if spec.fields[i] != f {
			t.Errorf("field %d = %v, want %v", i, spec.fields[i], f)
		}
	}
}

// TestDeriveComponentUntagged covers the zero-ceremony path: no tags at all, so
// every column is the snake case of its Go field name.
func TestDeriveComponentUntagged(t *testing.T) {
	type Ping struct {
		ID         int64
		RiskScore  float64
		Flagged    bool
		Settlement string
	}
	spec := componentOf(reflect.TypeFor[Ping]())

	want := []arrowipc.SchemaField{
		{Name: "id", Type: arrowipc.TypeInt64},
		{Name: "risk_score", Type: arrowipc.TypeFloat64},
		{Name: "flagged", Type: arrowipc.TypeBool},
		{Name: "settlement", Type: arrowipc.TypeUtf8},
	}
	if !sameFields(spec.fields, want) {
		t.Errorf("fields = %v, want %v", spec.fields, want)
	}
	if spec.name != "Ping" {
		t.Errorf("component name = %q, want Ping", spec.name)
	}
}

// TestDeriveComponentPanics covers the authoring mistakes. Every one of them is
// wrong for every batch, so the SDK refuses the row type rather than the input.
func TestDeriveComponentPanics(t *testing.T) {
	type unsupportedKind struct {
		ID     int64
		Amount float32
	}
	type unexported struct {
		ID     int64
		amount float64
	}
	type duplicateColumn struct {
		ID    int64 `saci:"id"`
		Other int64 `saci:"id"`
	}
	type emptyTag struct {
		ID int64 `saci:""`
	}
	type embedded struct {
		unsupportedKind
		ID int64
	}
	type empty struct{}

	cases := []struct {
		name string
		typ  reflect.Type
		want string
	}{
		{"unsupported kind", reflect.TypeFor[unsupportedKind](), "Amount is a float32"},
		{"unexported field", reflect.TypeFor[unexported](), "amount is unexported"},
		{"duplicate column", reflect.TypeFor[duplicateColumn](), `declares column "id" twice`},
		{"empty tag", reflect.TypeFor[emptyTag](), "empty saci tag"},
		{"embedded field", reflect.TypeFor[embedded](), "embeds unsupportedKind"},
		{"no fields", reflect.TypeFor[empty](), "declares no fields"},
		{"not a struct", reflect.TypeFor[int64](), "is a int64, want a struct"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				raised := recover()
				if raised == nil {
					t.Fatalf("derivation accepted %s, want a panic naming %q", c.typ, c.want)
				}
				message, ok := raised.(string)
				if !ok {
					t.Fatalf("panicked with %T, want a string", raised)
				}
				if !strings.Contains(message, c.want) {
					t.Errorf("panic = %q, want it to name %q", message, c.want)
				}
			}()
			deriveComponent(c.typ)
		})
	}
}

// TestFingerprint pins the values the host computes for the same components.
//
// The algorithm is a cross-language contract, so these two are the regression
// gate: the twelve-field `Order` the polyglot chain uses, and a one-field
// component that catches a mistake in the version bytes or the offset basis
// which a longer field list could mask.
func TestFingerprint(t *testing.T) {
	order := componentOf(reflect.TypeFor[Order]())
	if got, want := fingerprint([]component{order}), "f6405a7b"; got != want {
		t.Errorf("fingerprint(Order) = %s, want %s", got, want)
	}

	single := component{
		name:    "X",
		version: 1,
		fields:  []arrowipc.SchemaField{{Name: "x", Type: arrowipc.TypeInt64}},
	}
	if got, want := fingerprint([]component{single}), "43623dda"; got != want {
		t.Errorf("fingerprint(X) = %s, want %s", got, want)
	}
}

// TestFingerprintIsOrderSensitive is why the derivation never sorts fields: the
// same names in a different order are a different component.
func TestFingerprintIsOrderSensitive(t *testing.T) {
	forward := component{name: "X", version: 1, fields: []arrowipc.SchemaField{
		{Name: "a", Type: arrowipc.TypeInt64},
		{Name: "b", Type: arrowipc.TypeInt64},
	}}
	reversed := component{name: "X", version: 1, fields: []arrowipc.SchemaField{
		forward.fields[1], forward.fields[0],
	}}
	if fingerprint([]component{forward}) == fingerprint([]component{reversed}) {
		t.Error("reordering the fields left the fingerprint unchanged")
	}

	// A type change does not: the hash covers names and versions only.
	retyped := component{name: "X", version: 1, fields: []arrowipc.SchemaField{
		{Name: "a", Type: arrowipc.TypeUtf8},
		{Name: "b", Type: arrowipc.TypeInt64},
	}}
	if fingerprint([]component{forward}) != fingerprint([]component{retyped}) {
		t.Error("changing a field's type changed the fingerprint")
	}
}
