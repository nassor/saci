// The host-io capabilities a transform may reach for: logging, metrics, config.
//
// Why these are delegates rather than direct calls
//
// `host-io` only exists inside a component: the wit-bindgen output that declares
// IHostIoImports is generated into the processor project, not into this package.
// The generated export glue therefore hands the three functions over as
// delegates, and this package stays a plain net10.0 library that the SDK's own
// tests can drive without a wasm runtime.
//
// Why the accessor is static
//
// A transform's signature is `(TRow row)` or `(TRow row, SaciConfig config)`;
// threading a context object through it would be the ceremony this SDK exists to
// remove. SaciRuntime binds the current batch's host before the first transform
// runs and unbinds it afterwards, so a call outside a batch fails loudly instead
// of writing into a stale channel. The binding is thread-static: a component
// instance is single-threaded, and the SDK's host-side tests are not.

using System.Globalization;
using System.Text;

namespace Saci.Sdk;

/// <summary>`host-io::log-level`.</summary>
public enum SaciLogLevel
{
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// <summary>The three `host-io` functions, as the generated export glue supplies
/// them.</summary>
public sealed class SaciHostIo
{
    public SaciHostIo(
        Action<SaciLogLevel, string, string> log,
        Action<string, double> metric,
        Func<string, string?> config)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(metric);
        ArgumentNullException.ThrowIfNull(config);
        LogSink = log;
        MetricSink = metric;
        ConfigSink = config;
    }

    internal Action<SaciLogLevel, string, string> LogSink { get; }

    internal Action<string, double> MetricSink { get; }

    internal Func<string, string?> ConfigSink { get; }
}

/// <summary>Observability from inside a transform.</summary>
public static class SaciHost
{
    [ThreadStatic]
    private static SaciHostIo? _io;

    [ThreadStatic]
    private static List<Counter>? _counters;

    /// <summary>Writes one structured log line.</summary>
    public static void Log(SaciLogLevel level, string target, string message)
    {
        ArgumentException.ThrowIfNullOrEmpty(target);
        Current().LogSink(level, target, message ?? string.Empty);
    }

    /// <summary>Observes a metric immediately, once per call.</summary>
    public static void Metric(string name, double value)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        Current().MetricSink(name, value);
    }

    /// <summary>Adds one to a batch-scoped counter.</summary>
    /// <remarks>A transform sees one row at a time, so a per-row
    /// <see cref="Metric"/> call would report a batch of six rows as six
    /// observations of 1. Counters accumulate instead and are reported once per
    /// batch, as a single observation of the total, which is what a per-batch
    /// count means to the host's `saci_processor_metric` histogram.</remarks>
    public static void Count(string name) => Count(name, 1.0);

    /// <summary>Adds <paramref name="value"/> to a batch-scoped counter.</summary>
    public static void Count(string name, double value)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        _ = Current();
        List<Counter> counters = _counters ??= [];
        for (int i = 0; i < counters.Count; i++)
        {
            if (counters[i].Name == name)
            {
                counters[i].Total += value;
                return;
            }
        }
        counters.Add(new Counter(name, value));
    }

    /// <summary>Binds the host for one batch.</summary>
    internal static void Begin(SaciHostIo io)
    {
        _io = io;
        _counters = null;
    }

    /// <summary>Reports every counter as one metric observation and renders them
    /// for the batch's summary log line.</summary>
    internal static string Flush()
    {
        SaciHostIo io = Current();
        List<Counter>? counters = _counters;
        _counters = null;
        if (counters is null || counters.Count == 0)
        {
            return string.Empty;
        }
        StringBuilder rendered = new();
        for (int i = 0; i < counters.Count; i++)
        {
            io.MetricSink(counters[i].Name, counters[i].Total);
            rendered.Append(", ")
                .Append(counters[i].Name)
                .Append('=')
                .Append(counters[i].Total.ToString("G", CultureInfo.InvariantCulture));
        }
        return rendered.ToString();
    }

    /// <summary>Unbinds the host, so a call from outside a batch fails.</summary>
    internal static void End()
    {
        _io = null;
        _counters = null;
    }

    private static SaciHostIo Current() =>
        _io ?? throw new SaciProcessorException(
            "SaciHost is only bound while run-batch is executing");

    private sealed class Counter(string name, double total)
    {
        internal readonly string Name = name;
        internal double Total = total;
    }
}
