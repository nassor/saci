// The wit-bindgen output the generated export glue compiles against, hand-written
// so the generator can be tested without a wasm toolchain.
//
// These declarations mirror what `wit-bindgen` 0.58.0 emits for world
// `saci-pipeline` of package `saci:pipeline@0.3.0`, as componentize-dotnet drops it
// into obj/.../wit_bindgen/ before csc runs: member names, casing, nesting and
// the WitException error channel included. If wit-bindgen's C# backend changes any
// of those, this file stops compiling against the generated export and this test
// project is where that shows up, rather than in a 535 MB wasi-sdk build.
//
// The three host-io functions delegate to WitHost, which the tests read back.

using System.Collections.Generic;

namespace SaciPipelineWorld
{
    /// <summary>The error channel a `result<T, E>` return lowers onto.</summary>
    public class WitException : global::System.Exception
    {
        public WitException(object value, uint nestingLevel)
        {
            Value = value;
            NestingLevel = nestingLevel;
        }

        public object Value { get; }

        public uint NestingLevel { get; }
    }

    public class WitException<T> : WitException
    {
        public WitException(T value, uint nestingLevel) : base(value!, nestingLevel) { }

        public T TypedValue => (T)Value;
    }
}

namespace SaciPipelineWorld.wit.Imports.saci.pipeline.v0_3_0
{
    public interface ITypesImports
    {
        public struct ComponentDescriptor
        {
            public string name;
            public byte[] arrowSchemaIpc;

            public ComponentDescriptor(string name, byte[] arrowSchemaIpc)
            {
                this.name = name;
                this.arrowSchemaIpc = arrowSchemaIpc;
            }
        }

        public struct PipelineDescriptor
        {
            public string name;
            public string version;
            public List<ComponentDescriptor> components;
            public bool stateful;
            public string schemaFingerprint;

            public PipelineDescriptor(
                string name,
                string version,
                List<ComponentDescriptor> components,
                bool stateful,
                string schemaFingerprint)
            {
                this.name = name;
                this.version = version;
                this.components = components;
                this.stateful = stateful;
                this.schemaFingerprint = schemaFingerprint;
            }
        }

        public struct RunMetrics
        {
            public ulong wallNs;
            public ulong rowsIn;
            public ulong rowsOut;
            public uint systemsRun;
            public uint retries;

            public RunMetrics(ulong wallNs, ulong rowsIn, ulong rowsOut, uint systemsRun, uint retries)
            {
                this.wallNs = wallNs;
                this.rowsIn = rowsIn;
                this.rowsOut = rowsOut;
                this.systemsRun = systemsRun;
                this.retries = retries;
            }
        }

        public struct RunResult
        {
            public byte[] output;
            public byte[]? checkpoint;
            public RunMetrics metrics;
            public List<string>? routes;

            public RunResult(
                byte[] output, byte[]? checkpoint, RunMetrics metrics, List<string>? routes)
            {
                this.output = output;
                this.checkpoint = checkpoint;
                this.metrics = metrics;
                this.routes = routes;
            }
        }

        public class RunError
        {
            public readonly byte Tag;
            private readonly object? value;

            private RunError(byte tag, object? value)
            {
                Tag = tag;
                this.value = value;
            }

            public static RunError Retryable(string retryable) => new RunError(Tags.Retryable, retryable);

            public static RunError Permanent(string permanent) => new RunError(Tags.Permanent, permanent);

            public static RunError SchemaMismatch(string schemaMismatch) =>
                new RunError(Tags.SchemaMismatch, schemaMismatch);

            public string AsPermanent => Tag == Tags.Permanent
                ? (string)value!
                : throw new global::System.ArgumentException("expected Permanent, got " + Tag);

            public class Tags
            {
                public const byte Retryable = 0;
                public const byte Permanent = 1;
                public const byte SchemaMismatch = 2;
            }
        }
    }

    public interface IHostIoImports
    {
        public enum LogLevel
        {
            TRACE,
            DEBUG,
            INFO,
            WARN,
            ERROR,
        }

        public static void Log(LogLevel level, string target, string message) =>
            global::Saci.Sdk.Tests.WitHost.Log(level, target, message);

        public static void Metric(string name, double value) =>
            global::Saci.Sdk.Tests.WitHost.Metric(name, value);

        public static string? GetConfig(string key) => global::Saci.Sdk.Tests.WitHost.GetConfig(key);
    }
}

namespace SaciPipelineWorld.wit.Exports.saci.pipeline.v0_3_0
{
    public interface IPipelineExports
    {
        static abstract global::SaciPipelineWorld.wit.Imports.saci.pipeline.v0_3_0.ITypesImports.PipelineDescriptor
            Describe();

        static abstract global::SaciPipelineWorld.wit.Imports.saci.pipeline.v0_3_0.ITypesImports.RunResult
            RunBatch(byte[] input, byte[]? prior);
    }
}

namespace Saci.Sdk.Tests
{
    /// <summary>Stands in for the host behind `host-io`: the config map the
    /// processor reads, and the log lines and metrics it writes.</summary>
    internal static class WitHost
    {
        internal static readonly Dictionary<string, string> Config = new(StringComparer.Ordinal);

        internal static readonly List<(SaciPipelineWorld.wit.Imports.saci.pipeline.v0_3_0.IHostIoImports.LogLevel Level, string Target, string Message)> Logs = [];

        internal static readonly List<(string Name, double Value)> Metrics = [];

        internal static void Reset()
        {
            Config.Clear();
            Logs.Clear();
            Metrics.Clear();
        }

        internal static void Log(
            SaciPipelineWorld.wit.Imports.saci.pipeline.v0_3_0.IHostIoImports.LogLevel level,
            string target,
            string message) => Logs.Add((level, target, message));

        internal static void Metric(string name, double value) => Metrics.Add((name, value));

        internal static string? GetConfig(string key) =>
            Config.TryGetValue(key, out string? value) ? value : null;
    }
}
