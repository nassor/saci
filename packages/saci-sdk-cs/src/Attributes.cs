// The attributes a processor author applies, and the only thing they have to
// learn to write one.
//
// These types carry no behaviour. They are read at compile time by the
// incremental generator in generator/, which turns them into the
// `saci:pipeline/pipeline` export glue: a row class becomes an Arrow schema plus
// a pair of accessor delegates per field, and a transform method becomes a call
// site. Nothing reads them at run time, because `GetCustomAttributes` under
// PublishTrimmed and NativeAOT-LLVM is exactly the pattern that silently loses
// members.

namespace Saci.Sdk;

/// <summary>Marks a class as one SACI component: its properties are the
/// component's Arrow schema, in declaration order.</summary>
/// <remarks>The wire component name is the class name, because that is what the
/// host registers and what `run-batch` dispatches on. The class needs an
/// accessible parameterless constructor and settable properties: the SDK
/// materialises one instance per row of every batch.</remarks>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class SaciComponentAttribute : Attribute
{
    /// <summary>The component's schema version, which the host compares against
    /// its own registration and mixes into the schema fingerprint.</summary>
    /// <remarks>Defaults to 1, matching `saci_core::Component::version`.</remarks>
    public uint Version { get; set; } = 1;
}

/// <summary>Overrides the wire name of one property.</summary>
/// <remarks>Without it the wire name is the snake_case form of the property
/// name, which is what the Rust component definitions use.</remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
public sealed class SaciFieldAttribute(string name) : Attribute
{
    /// <summary>The Arrow field name.</summary>
    public string Name { get; } = name;
}

/// <summary>Marks a static method as a transform: it runs once per row of the
/// component its first parameter names.</summary>
/// <remarks>The method takes `(TRow row)` or `(TRow row, SaciConfig config)` and
/// returns void. It mutates the row; the SDK re-encodes every column afterwards,
/// so a transform may write any field, including a string.</remarks>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class SaciTransformAttribute : Attribute
{
    /// <summary>Sorts this transform against the others. Transforms run in
    /// source declaration order within one <see cref="Order"/>, and every
    /// transform of a component runs over the whole batch before the next one
    /// starts, the way a saci System does.</summary>
    public int Order { get; set; }
}

/// <summary>Declares the assembly a SACI processor, and names it.</summary>
/// <remarks>Required: the generator emits nothing without it, and the host keys
/// config and checkpoint compatibility on the reported name and version.</remarks>
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = false)]
public sealed class SaciProcessorAttribute(string name, string version) : Attribute
{
    /// <summary>`pipeline-descriptor.name`.</summary>
    public string Name { get; } = name;

    /// <summary>`pipeline-descriptor.version`. This is the processor's version,
    /// not any component's schema version.</summary>
    public string Version { get; } = version;

    /// <summary>The `host-io::log` target the host bridges onto tracing.
    /// Defaults to <see cref="Name"/>.</summary>
    public string? LogTarget { get; set; }
}
