// What the generator reads off the symbols, and the diagnostics it reports when
// it cannot.
//
// Every model here is built from an ISymbol during the syntax pass and holds
// nothing but strings and enums, so the emit pass never touches the compilation
// again. That also means an author's mistake becomes a compile error with a
// location rather than a runtime failure inside a wasm component, which is the
// only reason to prefer a generator over reflection in the first place.

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

using Microsoft.CodeAnalysis;

namespace Saci.Sdk.Generators;

/// <summary>The four Arrow types the wire format has discriminants for.</summary>
internal enum FieldKind
{
    Int64,
    Float64,
    Bool,
    Utf8,
}

/// <summary>Attribute metadata names, and the factory each field kind maps to.</summary>
internal static class Names
{
    internal const string ComponentAttribute = "Saci.Sdk.SaciComponentAttribute";
    internal const string FieldAttribute = "Saci.Sdk.SaciFieldAttribute";
    internal const string TransformAttribute = "Saci.Sdk.SaciTransformAttribute";
    internal const string ProcessorAttribute = "Saci.Sdk.SaciProcessorAttribute";
    internal const string ConfigType = "global::Saci.Sdk.SaciConfig";

    /// <summary>The generated namespace and class name wit-bindgen's C# backend
    /// dictates for the `pipeline` export of world `saci-pipeline`, package
    /// `saci:pipeline@0.3.0`.</summary>
    internal const string ExportNamespace = "SaciPipelineWorld.wit.Exports.saci.pipeline.v0_3_0";

    internal const string ExportClass = "PipelineExportsImpl";

    internal const string ImportsNamespace =
        "global::SaciPipelineWorld.wit.Imports.saci.pipeline.v0_3_0";

    internal static string Factory(FieldKind kind) => kind switch
    {
        FieldKind.Int64 => "Int64",
        FieldKind.Float64 => "Float64",
        FieldKind.Bool => "Bool",
        _ => "Utf8",
    };

    /// <summary>The snake_case wire name a property gets without an explicit
    /// <c>[SaciField]</c>: the convention the Rust component definitions use.</summary>
    /// <remarks>An underscore goes before an upper-case letter that follows a
    /// lower-case one or a digit, and before the last letter of an acronym that
    /// starts a word, so `UsdAmountDisplay` is `usd_amount_display` and
    /// `USDAmount` is `usd_amount`.</remarks>
    internal static string SnakeCase(string name)
    {
        StringBuilder snake = new StringBuilder(name.Length + 8);
        for (int i = 0; i < name.Length; i++)
        {
            char c = name[i];
            if (!char.IsUpper(c))
            {
                snake.Append(c);
                continue;
            }
            bool afterLower = i > 0 && (char.IsLower(name[i - 1]) || char.IsDigit(name[i - 1]));
            bool startsWord = i > 0
                && char.IsUpper(name[i - 1])
                && i + 1 < name.Length
                && char.IsLower(name[i + 1]);
            if (afterLower || startsWord)
            {
                snake.Append('_');
            }
            snake.Append(char.ToLowerInvariant(c));
        }
        return snake.ToString();
    }
}

/// <summary>The compile errors an author can hit.</summary>
internal static class Rules
{
    private const string Category = "Saci.Sdk";

    internal static readonly DiagnosticDescriptor NoProcessor = new DiagnosticDescriptor(
        "SACI0001",
        "Assembly declares no SACI processor",
        "This assembly declares [SaciComponent] or [SaciTransform] but no "
        + "[assembly: SaciProcessor(name, version)], so no pipeline export can be generated",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor NoComponent = new DiagnosticDescriptor(
        "SACI0002",
        "SACI processor declares no component",
        "[assembly: SaciProcessor] needs at least one [SaciComponent] class: a component's "
        + "properties are the Arrow schema the processor reads and writes",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor RowMustBeAClass = new DiagnosticDescriptor(
        "SACI0003",
        "SACI component must be a reference type",
        "'{0}' is a value type: a transform mutates the row it is handed, so a component has "
        + "to be a class or a record class",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor RowNeedsConstructor = new DiagnosticDescriptor(
        "SACI0004",
        "SACI component needs a parameterless constructor",
        "'{0}' has no accessible parameterless constructor: the SDK materialises one instance "
        + "per row of every batch",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor PropertyNotSettable = new DiagnosticDescriptor(
        "SACI0005",
        "SACI component property must be settable",
        "'{0}.{1}' has an init-only setter, so the SDK cannot write the decoded value into it; "
        + "declare it as '{{ get; set; }}'",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor PropertyTypeUnsupported = new DiagnosticDescriptor(
        "SACI0006",
        "SACI component property has no Arrow type",
        "'{0}.{1}' is '{2}': a component property must be long, double, bool or string, which "
        + "are the wire format's Int64, Float64, Boolean and Utf8",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor ComponentDeclaredTwice = new DiagnosticDescriptor(
        "SACI0007",
        "SACI component name is declared twice",
        "Two classes claim the component name '{0}': the wire name is the class name, and the "
        + "host dispatches on it",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor TransformSignature = new DiagnosticDescriptor(
        "SACI0008",
        "SACI transform has the wrong signature",
        "'{0}' must be 'static void {0}(TRow row)' or 'static void {0}(TRow row, SaciConfig "
        + "config)' where TRow is a [SaciComponent] class",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor TransformRowUnknown = new DiagnosticDescriptor(
        "SACI0009",
        "SACI transform names a type that is not a component",
        "'{0}' takes '{1}', which is not marked [SaciComponent]",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor ComponentHasNoFields = new DiagnosticDescriptor(
        "SACI0010",
        "SACI component declares no fields",
        "Component '{0}' has no public settable property of a supported type, and its "
        + "properties are its Arrow schema",
        Category, DiagnosticSeverity.Error, isEnabledByDefault: true);
}

/// <summary>One property of a component.</summary>
internal sealed class FieldModel
{
    internal FieldModel(string wireName, string propertyName, FieldKind kind)
    {
        WireName = wireName;
        PropertyName = propertyName;
        Kind = kind;
    }

    internal string WireName { get; }

    internal string PropertyName { get; }

    internal FieldKind Kind { get; }
}

/// <summary>One `[SaciComponent]` class.</summary>
internal sealed class ComponentModel
{
    private ComponentModel(
        string typeName,
        string componentName,
        uint version,
        ImmutableArray<FieldModel> fields,
        ImmutableArray<Diagnostic> diagnostics)
    {
        TypeName = typeName;
        ComponentName = componentName;
        Version = version;
        Fields = fields;
        Diagnostics = diagnostics;
    }

    /// <summary>Fully qualified, `global::` prefixed.</summary>
    internal string TypeName { get; }

    /// <summary>The wire component name, which is the class name.</summary>
    internal string ComponentName { get; }

    internal uint Version { get; }

    internal ImmutableArray<FieldModel> Fields { get; }

    internal ImmutableArray<Diagnostic> Diagnostics { get; }

    internal static ComponentModel From(GeneratorAttributeSyntaxContext context)
    {
        INamedTypeSymbol type = (INamedTypeSymbol)context.TargetSymbol;
        ImmutableArray<Diagnostic>.Builder diagnostics = ImmutableArray.CreateBuilder<Diagnostic>();
        Location location = context.TargetNode.GetLocation();

        uint version = 1;
        foreach (KeyValuePair<string, TypedConstant> argument in context.Attributes[0].NamedArguments)
        {
            if (argument.Key == "Version" && argument.Value.Value is uint declared)
            {
                version = declared;
            }
        }

        if (type.IsValueType)
        {
            diagnostics.Add(Diagnostic.Create(Rules.RowMustBeAClass, location, type.Name));
        }
        else if (!type.InstanceConstructors.Any(c =>
                     c.Parameters.Length == 0 && c.DeclaredAccessibility == Accessibility.Public))
        {
            diagnostics.Add(Diagnostic.Create(Rules.RowNeedsConstructor, location, type.Name));
        }

        ImmutableArray<FieldModel>.Builder fields = ImmutableArray.CreateBuilder<FieldModel>();
        foreach (ISymbol member in type.GetMembers())
        {
            if (member is not IPropertySymbol property
                || property.IsStatic
                || property.IsIndexer
                || property.DeclaredAccessibility != Accessibility.Public
                || property.GetMethod is null)
            {
                continue;
            }
            if (property.SetMethod is null
                || property.SetMethod.DeclaredAccessibility != Accessibility.Public)
            {
                // A computed or read-only property is not part of the schema.
                continue;
            }
            if (property.SetMethod.IsInitOnly)
            {
                diagnostics.Add(Diagnostic.Create(
                    Rules.PropertyNotSettable,
                    property.Locations.FirstOrDefault() ?? location,
                    type.Name,
                    property.Name));
                continue;
            }
            FieldKind? kind = KindOf(property.Type);
            if (kind is null)
            {
                diagnostics.Add(Diagnostic.Create(
                    Rules.PropertyTypeUnsupported,
                    property.Locations.FirstOrDefault() ?? location,
                    type.Name,
                    property.Name,
                    property.Type.ToDisplayString()));
                continue;
            }
            fields.Add(new FieldModel(WireNameOf(property), property.Name, kind.Value));
        }

        return new ComponentModel(
            type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            type.Name,
            version,
            fields.ToImmutable(),
            diagnostics.ToImmutable());
    }

    private static FieldKind? KindOf(ITypeSymbol type) => type.SpecialType switch
    {
        SpecialType.System_Int64 => FieldKind.Int64,
        SpecialType.System_Double => FieldKind.Float64,
        SpecialType.System_Boolean => FieldKind.Bool,
        SpecialType.System_String => FieldKind.Utf8,
        _ => null,
    };

    private static string WireNameOf(IPropertySymbol property)
    {
        foreach (AttributeData attribute in property.GetAttributes())
        {
            if (attribute.AttributeClass?.ToDisplayString() != Names.FieldAttribute
                || attribute.ConstructorArguments.Length != 1)
            {
                continue;
            }
            if (attribute.ConstructorArguments[0].Value is string name && name.Length > 0)
            {
                return name;
            }
        }
        return Names.SnakeCase(property.Name);
    }
}

/// <summary>One `[SaciTransform]` method.</summary>
internal sealed class TransformModel
{
    private TransformModel(
        string rowTypeName,
        string rowTypeDisplay,
        string invocation,
        int parameters,
        int order,
        string sortPath,
        int sortLine,
        Location location,
        ImmutableArray<Diagnostic> diagnostics)
    {
        RowTypeName = rowTypeName;
        RowTypeDisplay = rowTypeDisplay;
        Invocation = invocation;
        Parameters = parameters;
        Order = order;
        SortPath = sortPath;
        SortLine = sortLine;
        Location = location;
        Diagnostics = diagnostics;
    }

    /// <summary>Fully qualified row type, matched against
    /// <see cref="ComponentModel.TypeName"/>.</summary>
    internal string RowTypeName { get; }

    internal string RowTypeDisplay { get; }

    /// <summary>Fully qualified `Type.Method`, ready to be called.</summary>
    internal string Invocation { get; }

    /// <summary>One for `(row)`, two for `(row, config)`.</summary>
    internal int Parameters { get; }

    internal int Order { get; }

    internal string SortPath { get; }

    internal int SortLine { get; }

    internal Location Location { get; }

    internal ImmutableArray<Diagnostic> Diagnostics { get; }

    /// <summary>Declaration order, with an explicit `Order` taking precedence.</summary>
    internal static int Compare(TransformModel a, TransformModel b)
    {
        int byOrder = a.Order.CompareTo(b.Order);
        if (byOrder != 0)
        {
            return byOrder;
        }
        int byPath = string.CompareOrdinal(a.SortPath, b.SortPath);
        return byPath != 0 ? byPath : a.SortLine.CompareTo(b.SortLine);
    }

    internal static TransformModel From(GeneratorAttributeSyntaxContext context)
    {
        IMethodSymbol method = (IMethodSymbol)context.TargetSymbol;
        ImmutableArray<Diagnostic>.Builder diagnostics = ImmutableArray.CreateBuilder<Diagnostic>();
        Location location = context.TargetNode.GetLocation();
        FileLinePositionSpan span = location.GetLineSpan();

        int order = 0;
        foreach (KeyValuePair<string, TypedConstant> argument in context.Attributes[0].NamedArguments)
        {
            if (argument.Key == "Order" && argument.Value.Value is int declared)
            {
                order = declared;
            }
        }

        bool shaped = method.IsStatic
            && method.ReturnsVoid
            && (method.Parameters.Length == 1 || method.Parameters.Length == 2)
            && method.Parameters[0].RefKind == RefKind.None;
        if (shaped && method.Parameters.Length == 2)
        {
            shaped = method.Parameters[1].Type
                .ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) == Names.ConfigType;
        }
        if (!shaped)
        {
            diagnostics.Add(Diagnostic.Create(Rules.TransformSignature, location, method.Name));
        }

        ITypeSymbol? row = method.Parameters.Length > 0 ? method.Parameters[0].Type : null;
        return new TransformModel(
            row is null ? string.Empty : row.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            row is null ? string.Empty : row.ToDisplayString(),
            method.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
                + "." + method.Name,
            method.Parameters.Length,
            order,
            span.Path,
            span.StartLinePosition.Line,
            location,
            diagnostics.ToImmutable());
    }
}

/// <summary>`[assembly: SaciProcessor]`.</summary>
internal sealed class ProcessorModel
{
    private ProcessorModel(string name, string version, string logTarget)
    {
        Name = name;
        Version = version;
        LogTarget = logTarget;
    }

    internal string Name { get; }

    internal string Version { get; }

    internal string LogTarget { get; }

    internal static ProcessorModel? From(Compilation compilation)
    {
        foreach (AttributeData attribute in compilation.Assembly.GetAttributes())
        {
            if (attribute.AttributeClass?.ToDisplayString() != Names.ProcessorAttribute
                || attribute.ConstructorArguments.Length != 2)
            {
                continue;
            }
            if (attribute.ConstructorArguments[0].Value is not string name
                || attribute.ConstructorArguments[1].Value is not string version
                || name.Length == 0
                || version.Length == 0)
            {
                continue;
            }
            string logTarget = name;
            foreach (KeyValuePair<string, TypedConstant> argument in attribute.NamedArguments)
            {
                if (argument.Key == "LogTarget"
                    && argument.Value.Value is string target
                    && target.Length > 0)
                {
                    logTarget = target;
                }
            }
            return new ProcessorModel(name, version, logTarget);
        }
        return null;
    }
}
