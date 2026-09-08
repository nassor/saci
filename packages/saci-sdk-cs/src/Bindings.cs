// What the generator bakes into the processor assembly: one binding per
// component, holding a compiled delegate per property accessor and per transform.
//
// This is the whole reason the SDK needs no reflection. A field binding knows,
// at compile time, that `id` is an Int64 reached through `r => r.Id` and
// `(r, v) => r.Id = v`, so decoding a batch into row objects and re-encoding it
// afterwards is a pair of delegate calls per value. Nothing here inspects a Type,
// which is what makes the processor safe under PublishTrimmed and NativeAOT-LLVM.
//
// A processor author never writes these types. They are public because generated
// code in another assembly constructs them, and internal where only SaciRuntime
// calls them, which also stops anything outside this package from deriving a
// fifth Arrow type the wire format has no discriminant for.

using Saci.ArrowIpc;

namespace Saci.Sdk;

/// <summary>One property of a component, bound to its wire name.</summary>
public abstract class SaciFieldBinding
{
    internal SaciFieldBinding(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        Name = name;
    }

    /// <summary>The Arrow field name.</summary>
    public string Name { get; }
}

/// <summary>One property of <typeparamref name="TRow"/>, with both accessors.</summary>
public abstract class SaciFieldBinding<TRow> : SaciFieldBinding
    where TRow : class
{
    internal SaciFieldBinding(string name) : base(name) { }

    /// <summary>Decodes this column and writes it into every row.</summary>
    internal abstract void Load(ArrowBatch batch, TRow[] rows);

    /// <summary>Reads this property off every row, ready to be re-encoded. An
    /// empty row array yields an empty column, which is how a schema is
    /// described without any values.</summary>
    internal abstract Column Emit(TRow[] rows);
}

/// <summary>Binds one property to one Arrow type.</summary>
/// <remarks>The property's CLR type picks the factory: long is Int64, double is
/// Float64, bool is Boolean, string is Utf8. Those four are the wire format's
/// whole type set.</remarks>
public static class SaciField
{
    public static SaciFieldBinding<TRow> Int64<TRow>(
        string name, Func<TRow, long> get, Action<TRow, long> set)
        where TRow : class => new Int64FieldBinding<TRow>(name, get, set);

    public static SaciFieldBinding<TRow> Float64<TRow>(
        string name, Func<TRow, double> get, Action<TRow, double> set)
        where TRow : class => new Float64FieldBinding<TRow>(name, get, set);

    public static SaciFieldBinding<TRow> Bool<TRow>(
        string name, Func<TRow, bool> get, Action<TRow, bool> set)
        where TRow : class => new BoolFieldBinding<TRow>(name, get, set);

    public static SaciFieldBinding<TRow> Utf8<TRow>(
        string name, Func<TRow, string> get, Action<TRow, string> set)
        where TRow : class => new Utf8FieldBinding<TRow>(name, get, set);
}

internal sealed class Int64FieldBinding<TRow>(
    string name, Func<TRow, long> get, Action<TRow, long> set)
    : SaciFieldBinding<TRow>(name)
    where TRow : class
{
    internal override void Load(ArrowBatch batch, TRow[] rows)
    {
        long[] values = batch.Int64s(Name);
        for (int i = 0; i < rows.Length; i++)
        {
            set(rows[i], values[i]);
        }
    }

    internal override Column Emit(TRow[] rows)
    {
        long[] values = new long[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values[i] = get(rows[i]);
        }
        return new Int64Column(Name, values);
    }
}

internal sealed class Float64FieldBinding<TRow>(
    string name, Func<TRow, double> get, Action<TRow, double> set)
    : SaciFieldBinding<TRow>(name)
    where TRow : class
{
    internal override void Load(ArrowBatch batch, TRow[] rows)
    {
        double[] values = batch.Float64s(Name);
        for (int i = 0; i < rows.Length; i++)
        {
            set(rows[i], values[i]);
        }
    }

    internal override Column Emit(TRow[] rows)
    {
        double[] values = new double[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values[i] = get(rows[i]);
        }
        return new Float64Column(Name, values);
    }
}

internal sealed class BoolFieldBinding<TRow>(
    string name, Func<TRow, bool> get, Action<TRow, bool> set)
    : SaciFieldBinding<TRow>(name)
    where TRow : class
{
    internal override void Load(ArrowBatch batch, TRow[] rows)
    {
        bool[] values = batch.Bools(Name);
        for (int i = 0; i < rows.Length; i++)
        {
            set(rows[i], values[i]);
        }
    }

    internal override Column Emit(TRow[] rows)
    {
        bool[] values = new bool[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values[i] = get(rows[i]);
        }
        return new BoolColumn(Name, values);
    }
}

internal sealed class Utf8FieldBinding<TRow>(
    string name, Func<TRow, string> get, Action<TRow, string> set)
    : SaciFieldBinding<TRow>(name)
    where TRow : class
{
    internal override void Load(ArrowBatch batch, TRow[] rows)
    {
        string[] values = batch.Strings(Name);
        for (int i = 0; i < rows.Length; i++)
        {
            set(rows[i], values[i]);
        }
    }

    internal override Column Emit(TRow[] rows)
    {
        string[] values = new string[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            // A Utf8 column is non-nullable on the wire, and an unset string
            // property is null rather than empty, so a transform that clears one
            // would otherwise fail deeper in the encoder with no field name.
            values[i] = get(rows[i])
                ?? throw new SaciProcessorException(
                    $"field \"{Name}\" row {i} is null, and a Utf8 column is non-nullable");
        }
        return new Utf8Column(Name, values);
    }
}

/// <summary>One component: its wire identity, its schema, and the transforms
/// that run over it.</summary>
public abstract class SaciComponentBinding
{
    internal SaciComponentBinding(string name, uint version)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        Name = name;
        Version = version;
    }

    /// <summary>The `__saci_component` label.</summary>
    public string Name { get; }

    /// <summary>The `__saci_schema_version` value.</summary>
    public uint Version { get; }

    /// <summary>Transforms bound to this component, reported to the host as
    /// `run-metrics.systems-run`.</summary>
    internal abstract int TransformCount { get; }

    /// <summary>Empty columns in schema order: names and Arrow types only.</summary>
    internal abstract Column[] SchemaColumns();

    /// <summary>Decodes this component out of <paramref name="input"/>, runs every
    /// transform over the whole batch, re-encodes it into
    /// <paramref name="output"/>, and returns the row count.</summary>
    internal abstract int Run(SaciStream input, SaciStream output, SaciConfig config);
}

/// <summary>The binding the generator constructs for one `[SaciComponent]`
/// class.</summary>
public sealed class SaciComponentBinding<TRow> : SaciComponentBinding
    where TRow : class
{
    private readonly Func<TRow> _create;
    private readonly SaciFieldBinding<TRow>[] _fields;
    private readonly Action<TRow, SaciConfig>[] _transforms;

    public SaciComponentBinding(
        string name,
        uint version,
        Func<TRow> create,
        SaciFieldBinding<TRow>[] fields,
        Action<TRow, SaciConfig>[] transforms)
        : base(name, version)
    {
        ArgumentNullException.ThrowIfNull(create);
        ArgumentNullException.ThrowIfNull(fields);
        ArgumentNullException.ThrowIfNull(transforms);
        if (fields.Length == 0)
        {
            throw new SaciProcessorException(
                $"component \"{name}\" declares no fields, and the fields are its schema");
        }
        _create = create;
        _fields = fields;
        _transforms = transforms;
    }

    internal override int TransformCount => _transforms.Length;

    internal override Column[] SchemaColumns()
    {
        TRow[] none = [];
        Column[] columns = new Column[_fields.Length];
        for (int i = 0; i < _fields.Length; i++)
        {
            columns[i] = _fields[i].Emit(none);
        }
        return columns;
    }

    internal override int Run(SaciStream input, SaciStream output, SaciConfig config)
    {
        ArrowBatch batch = input.Component(Name);
        TRow[] rows = new TRow[batch.Rows];
        for (int i = 0; i < rows.Length; i++)
        {
            rows[i] = _create();
        }
        for (int f = 0; f < _fields.Length; f++)
        {
            _fields[f].Load(batch, rows);
        }

        // Each transform sees the whole batch before the next one starts, which
        // is how a saci System runs and what makes a chain of transforms on one
        // component readable in declaration order.
        for (int t = 0; t < _transforms.Length; t++)
        {
            Action<TRow, SaciConfig> transform = _transforms[t];
            for (int i = 0; i < rows.Length; i++)
            {
                transform(rows[i], config);
            }
        }

        Column[] columns = new Column[_fields.Length];
        for (int f = 0; f < _fields.Length; f++)
        {
            columns[f] = _fields[f].Emit(rows);
        }
        output.WriteComponent(Name, Version, columns);
        return rows.Length;
    }
}
