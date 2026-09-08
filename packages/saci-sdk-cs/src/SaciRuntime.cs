// The two entry points the generated export glue calls, and the types it maps
// onto the WIT records.
//
// Why the WIT types are not mentioned here
//
// `ITypesImports.PipelineDescriptor` and friends are generated into the processor
// project by wit-bindgen, not into this package. SaciRuntime therefore returns its
// own plain records and the generated glue does the two-line translation, which
// keeps this package a normal net10.0 library its own tests can call.
//
// What run-batch does
//
// Decode every declared component into row objects, run each component's
// transforms over the whole batch, re-encode, and forward the `__alive` bitmap
// and any segment the processor did not declare untouched. That last part
// matters: the host replaces the whole partition dataset with what `run-batch`
// returns, so a dropped segment is lost data.
//
// State
//
// None. `prior` is ignored, `checkpoint` is null and `describe` reports
// `stateful: false`, which is the contract those three make together.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Text;

using Saci.ArrowIpc;

// Saci.Sdk and Saci.ArrowIpc share a parent namespace, so the bare name ArrowIpc
// binds to the namespace rather than to the codec's constants class.
using Arrow = Saci.ArrowIpc.ArrowIpc;

namespace Saci.Sdk;

/// <summary>A processor bug or a misconfiguration: the host's
/// `run-error::permanent` arm.</summary>
public sealed class SaciProcessorException : Exception
{
    public SaciProcessorException(string message) : base(message) { }

    public SaciProcessorException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>The identity the generator reads off `[assembly: SaciProcessor]`.</summary>
public sealed class SaciProcessorInfo
{
    public SaciProcessorInfo(string name, string version, string logTarget)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentException.ThrowIfNullOrEmpty(version);
        ArgumentException.ThrowIfNullOrEmpty(logTarget);
        Name = name;
        Version = version;
        LogTarget = logTarget;
    }

    public string Name { get; }

    public string Version { get; }

    public string LogTarget { get; }
}

/// <summary>One `component-descriptor`.</summary>
public sealed class SaciComponentSchema(string name, byte[] arrowSchemaIpc)
{
    public string Name { get; } = name;

    /// <summary>A schema-only Arrow IPC stream.</summary>
    public byte[] ArrowSchemaIpc { get; } = arrowSchemaIpc;
}

/// <summary>`pipeline-descriptor`, before the WIT record is built from it.</summary>
public sealed class SaciDescriptor(
    string name,
    string version,
    bool stateful,
    string schemaFingerprint,
    SaciComponentSchema[] components)
{
    public string Name { get; } = name;

    public string Version { get; } = version;

    public bool Stateful { get; } = stateful;

    /// <summary>Eight lowercase hex characters, as
    /// `saci_core::SchemaRegistry::fingerprint` computes them.</summary>
    public string SchemaFingerprint { get; } = schemaFingerprint;

    public SaciComponentSchema[] Components { get; } = components;
}

/// <summary>`run-result`, before the WIT record is built from it.</summary>
public sealed class SaciBatchResult(
    byte[] output,
    byte[]? checkpoint,
    ulong wallNs,
    ulong rowsIn,
    ulong rowsOut,
    uint systemsRun,
    uint retries)
{
    public byte[] Output { get; } = output;

    public byte[]? Checkpoint { get; } = checkpoint;

    public ulong WallNs { get; } = wallNs;

    public ulong RowsIn { get; } = rowsIn;

    public ulong RowsOut { get; } = rowsOut;

    public uint SystemsRun { get; } = systemsRun;

    public uint Retries { get; } = retries;
}

/// <summary>Implements `describe` and `run-batch` from generator-baked
/// bindings.</summary>
public static class SaciRuntime
{
    private const uint FnvOffsetBasis = 2166136261;
    private const uint FnvPrime = 16777619;

    /// <summary>Builds the descriptor: one schema-only Arrow IPC stream per
    /// component plus the schema fingerprint.</summary>
    /// <remarks>The fingerprint is computed here rather than embedded as a
    /// generated constant: it is a pure function of the component names, their
    /// schema versions and their field names, all of which the bindings already
    /// carry, so it cannot drift from the schema this processor actually
    /// reads.</remarks>
    public static SaciDescriptor Describe(
        SaciProcessorInfo processor, params SaciComponentBinding[] components)
    {
        ArgumentNullException.ThrowIfNull(processor);
        RequireDistinct(components);

        Column[][] schemas = new Column[components.Length][];
        SaciComponentSchema[] descriptors = new SaciComponentSchema[components.Length];
        for (int i = 0; i < components.Length; i++)
        {
            schemas[i] = components[i].SchemaColumns();
            descriptors[i] = new SaciComponentSchema(
                components[i].Name, Arrow.SchemaIpcStream(schemas[i]));
        }

        return new SaciDescriptor(
            processor.Name,
            processor.Version,
            stateful: false,
            Fingerprint(components, schemas),
            descriptors);
    }

    /// <summary>Runs one batch: decode, transform, re-encode.</summary>
    /// <exception cref="SaciProcessorException">The input is missing a component
    /// this processor declares, or a transform failed.</exception>
    /// <exception cref="ArrowIpcException">The input bytes are malformed.</exception>
    public static SaciBatchResult RunBatch(
        byte[] input,
        byte[]? prior,
        SaciHostIo host,
        SaciProcessorInfo processor,
        params SaciComponentBinding[] components)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(processor);
        RequireDistinct(components);
        _ = prior; // stateless: describe reports stateful: false

        long started = Stopwatch.GetTimestamp();
        SaciHost.Begin(host);
        try
        {
            SaciConfig config = new(host.ConfigSink);
            SaciStream parsed = new(input);
            bool[] alive = parsed.Component(Arrow.AliveComponent).Bools(Arrow.AliveField);

            SaciStream written = new();
            long rows = 0;
            int systems = 0;
            int handled = 0;
            foreach (string name in parsed.ComponentNames())
            {
                if (name == Arrow.AliveComponent)
                {
                    continue;
                }
                SaciComponentBinding? binding = Find(components, name);
                if (binding is null)
                {
                    written.WriteSegmentFrom(parsed, name);
                    continue;
                }
                rows += binding.Run(parsed, written, config);
                systems += binding.TransformCount;
                handled++;
            }
            if (handled != components.Length)
            {
                throw MissingComponent(parsed, components);
            }
            written.WriteAlive(alive);
            byte[] output = written.ToBytes();

            // Counters first: their totals belong in the same line that reports
            // the row count, and flushing is what turns them into metrics.
            string counters = SaciHost.Flush();
            host.LogSink(
                SaciLogLevel.Info,
                processor.LogTarget,
                string.Format(
                    CultureInfo.InvariantCulture,
                    "{0}: {1} rows, {2} transforms{3}",
                    processor.Name, rows, systems, counters));

            return new SaciBatchResult(
                output,
                checkpoint: null,
                wallNs: (ulong)Math.Max(0.0, Stopwatch.GetElapsedTime(started).TotalNanoseconds),
                rowsIn: (ulong)rows,
                rowsOut: (ulong)rows,
                systemsRun: (uint)systems,
                retries: 0);
        }
        finally
        {
            SaciHost.End();
        }
    }

    private static SaciComponentBinding? Find(SaciComponentBinding[] components, string name)
    {
        for (int i = 0; i < components.Length; i++)
        {
            if (components[i].Name == name)
            {
                return components[i];
            }
        }
        return null;
    }

    /// <summary>Names the declared component the input did not carry. The host
    /// writes one segment per registered component, so an absent one means the
    /// processor and the service disagree about the dataset.</summary>
    private static SaciProcessorException MissingComponent(
        SaciStream parsed, SaciComponentBinding[] components)
    {
        string[] present = parsed.ComponentNames();
        for (int i = 0; i < components.Length; i++)
        {
            if (Array.IndexOf(present, components[i].Name) < 0)
            {
                return new SaciProcessorException(
                    $"input carries no segment for declared component \"{components[i].Name}\"; "
                    + $"it carries {string.Join(", ", present)}");
            }
        }
        // Unreachable while every declared name is distinct, which RequireDistinct
        // enforces before either entry point runs.
        return new SaciProcessorException("input does not cover every declared component");
    }

    private static void RequireDistinct(SaciComponentBinding[] components)
    {
        ArgumentNullException.ThrowIfNull(components);
        if (components.Length == 0)
        {
            throw new SaciProcessorException("processor declares no components");
        }
        for (int i = 0; i < components.Length; i++)
        {
            if (components[i].Name == Arrow.AliveComponent)
            {
                throw new SaciProcessorException(
                    $"\"{Arrow.AliveComponent}\" is the liveness bitmap, not a component");
            }
            for (int j = 0; j < i; j++)
            {
                if (components[j].Name == components[i].Name)
                {
                    throw new SaciProcessorException(
                        $"component \"{components[i].Name}\" is declared twice");
                }
            }
        }
    }

    /// <summary>FNV-1a over component names, schema versions and field names,
    /// components sorted by name.</summary>
    /// <remarks>Byte for byte the algorithm in
    /// `docs/content/library/reference/wire-format.md`: no types and no nullability, so
    /// adding a field changes the fingerprint and retyping one does not.</remarks>
    private static string Fingerprint(SaciComponentBinding[] components, Column[][] schemas)
    {
        int[] order = new int[components.Length];
        for (int i = 0; i < order.Length; i++)
        {
            order[i] = i;
        }
        Array.Sort(order, (a, b) => string.CompareOrdinal(components[a].Name, components[b].Name));

        uint hash = FnvOffsetBasis;
        Span<byte> version = stackalloc byte[4];
        foreach (int i in order)
        {
            hash = Mix(hash, components[i].Name);
            BinaryPrimitives.WriteUInt32LittleEndian(version, components[i].Version);
            hash = Mix(hash, version);
            foreach (Column column in schemas[i])
            {
                hash = Mix(hash, column.Name);
            }
        }
        return hash.ToString("x8", CultureInfo.InvariantCulture);
    }

    private static uint Mix(uint hash, string text)
    {
        Span<byte> bytes = text.Length <= 64 ? stackalloc byte[256] : new byte[Encoding.UTF8.GetByteCount(text)];
        int written = Encoding.UTF8.GetBytes(text, bytes);
        return Mix(hash, bytes[..written]);
    }

    private static uint Mix(uint hash, ReadOnlySpan<byte> bytes)
    {
        for (int i = 0; i < bytes.Length; i++)
        {
            hash = (hash ^ bytes[i]) * FnvPrime;
        }
        return hash;
    }
}
