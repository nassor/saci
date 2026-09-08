package saci

import (
	"fmt"
	"reflect"

	arrowipc "github.com/nassor/saci/packages/saci-sdk-go/arrowipc"
)

// System is one unit of work in a processor's pipeline.
//
// The interface is closed: [Transform] is the only implementation, because a
// system that did not go through it would carry no row type and the SDK would
// have nothing to derive a schema from.
type System interface {
	// Name is what the batch summary and an error report call this system.
	Name() string

	// component is the row type this system reads and writes.
	component() component

	// run decodes the component if no earlier system did, then transforms every
	// row.
	run(*batch) error
}

// Transform registers a function that runs over every row of one component.
//
// The row type's fields are the component's columns and its Go name is the
// component name, so `Transform[Order]` operates on the host's `Order`. The
// function is handed a pointer, so a write to a field is a write to the column,
// and the whole component is re-encoded once every system has run.
//
// It panics when T is not a struct this SDK can map to a schema. That is an
// authoring mistake, and a stage that declares its processor as a package-level
// var hits it when the component is instantiated.
func Transform[T any](name string, fn func(row *T, cfg Config) error) System {
	return transform[T]{
		name: name,
		spec: componentOf(reflect.TypeFor[T]()),
		fn:   fn,
	}
}

type transform[T any] struct {
	name string
	spec component
	fn   func(row *T, cfg Config) error
}

func (t transform[T]) Name() string         { return t.name }
func (t transform[T]) component() component { return t.spec }

func (t transform[T]) run(b *batch) error {
	rows, err := rowsOf[T](b, t.spec)
	if err != nil {
		return err
	}
	for i := range *rows {
		if err := t.fn(&(*rows)[i], b.cfg); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
	}
	return nil
}

// rowsOf returns the batch's decoded rows for a component, decoding on first
// touch.
//
// The pointer is what makes a second system see the first one's writes: both
// hold the same slice header, so a transform that grows or replaces the slice
// would still be encoded.
func rowsOf[T any](b *batch, spec component) (*[]T, error) {
	if entry, ok := b.decoded[spec.name]; ok {
		rows, ok := entry.rows.(*[]T)
		if !ok {
			return nil, fmt.Errorf("component %s is already decoded as %T, and one component has one row type", spec.name, entry.rows)
		}
		return rows, nil
	}

	batch, err := b.stream.Component(spec.name)
	if err != nil {
		return nil, fmt.Errorf("read component %s: %w", spec.name, err)
	}
	decodedRows, err := decode[T](batch, spec)
	if err != nil {
		return nil, err
	}

	rows := &decodedRows
	b.decoded[spec.name] = &decoded{
		rows: rows,
		encode: func(w *arrowipc.Writer) error {
			return w.WriteComponent(spec.name, spec.version, columnsOf(*rows, spec)...)
		},
	}
	return rows, nil
}

// decode reads every column of a batch into a slice of row values.
//
// Column by column rather than row by row: each typed reader decodes a whole
// buffer in one pass, and the reflection cost is one field write per cell either
// way.
func decode[T any](b *arrowipc.Batch, spec component) ([]T, error) {
	rows := make([]T, b.Rows)
	view := reflect.ValueOf(rows)

	for k, f := range spec.fields {
		switch f.Type {
		case arrowipc.TypeInt64:
			values, err := b.Int64s(f.Name)
			if err != nil {
				return nil, fmt.Errorf("read %s.%s: %w", spec.name, f.Name, err)
			}
			for i, v := range values {
				view.Index(i).Field(k).SetInt(v)
			}
		case arrowipc.TypeFloat64:
			values, err := b.Float64s(f.Name)
			if err != nil {
				return nil, fmt.Errorf("read %s.%s: %w", spec.name, f.Name, err)
			}
			for i, v := range values {
				view.Index(i).Field(k).SetFloat(v)
			}
		case arrowipc.TypeBool:
			values, err := b.Bools(f.Name)
			if err != nil {
				return nil, fmt.Errorf("read %s.%s: %w", spec.name, f.Name, err)
			}
			for i, v := range values {
				view.Index(i).Field(k).SetBool(v)
			}
		case arrowipc.TypeUtf8:
			values, err := b.Strings(f.Name)
			if err != nil {
				return nil, fmt.Errorf("read %s.%s: %w", spec.name, f.Name, err)
			}
			for i, v := range values {
				view.Index(i).Field(k).SetString(v)
			}
		}
	}
	return rows, nil
}

// columnsOf gathers the row values back into columns, in schema order.
func columnsOf[T any](rows []T, spec component) []arrowipc.Column {
	view := reflect.ValueOf(rows)
	columns := make([]arrowipc.Column, len(spec.fields))

	for k, f := range spec.fields {
		switch f.Type {
		case arrowipc.TypeInt64:
			values := make([]int64, len(rows))
			for i := range values {
				values[i] = view.Index(i).Field(k).Int()
			}
			columns[k] = arrowipc.Int64Column{Name: f.Name, Values: values}
		case arrowipc.TypeFloat64:
			values := make([]float64, len(rows))
			for i := range values {
				values[i] = view.Index(i).Field(k).Float()
			}
			columns[k] = arrowipc.Float64Column{Name: f.Name, Values: values}
		case arrowipc.TypeBool:
			values := make([]bool, len(rows))
			for i := range values {
				values[i] = view.Index(i).Field(k).Bool()
			}
			columns[k] = arrowipc.BoolColumn{Name: f.Name, Values: values}
		case arrowipc.TypeUtf8:
			values := make([]string, len(rows))
			for i := range values {
				values[i] = view.Index(i).Field(k).String()
			}
			columns[k] = arrowipc.Utf8Column{Name: f.Name, Values: values}
		}
	}
	return columns
}
