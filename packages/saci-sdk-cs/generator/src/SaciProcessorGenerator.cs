// Emits the `saci:pipeline/pipeline` export glue for an assembly that declares
// [assembly: SaciProcessor] plus at least one [SaciComponent].
//
// Where the emitted code goes
//
// componentize-dotnet runs wit-bindgen as an MSBuild target before csc, so its
// output is already sitting in obj/.../wit_bindgen/ as ordinary <Compile> items
// when this generator runs. The generated file can therefore implement
// IPipelineExports directly, in the namespace and under the class name
// wit-bindgen's C# backend dictates: PipelineExportsInterop calls
// PipelineExportsImpl.Describe and PipelineExportsImpl.RunBatch by unqualified
// name from inside SaciPipelineWorld.wit.Exports.saci.pipeline.v0_3_0. Neither name
// is a choice, and neither is a partial class the author has to declare.
//
// What it bakes in
//
// One SaciComponentBinding per component, holding a getter and a setter delegate
// per property and one delegate per transform, all resolved at compile time.
// Nothing in the emitted code or in Saci.Sdk reads a Type or an attribute at run
// time, which is what keeps the processor correct under PublishTrimmed and
// NativeAOT-LLVM.

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Saci.Sdk.Generators;

[Generator(LanguageNames.CSharp)]
public sealed class SaciProcessorGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValueProvider<ImmutableArray<ComponentModel>> components =
            context.SyntaxProvider.ForAttributeWithMetadataName(
                    Names.ComponentAttribute,
                    static (node, _) => node is ClassDeclarationSyntax or RecordDeclarationSyntax,
                    static (syntax, _) => ComponentModel.From(syntax))
                .Collect();

        IncrementalValueProvider<ImmutableArray<TransformModel>> transforms =
            context.SyntaxProvider.ForAttributeWithMetadataName(
                    Names.TransformAttribute,
                    static (node, _) => node is MethodDeclarationSyntax,
                    static (syntax, _) => TransformModel.From(syntax))
                .Collect();

        // The assembly attribute is read off the compilation rather than through
        // ForAttributeWithMetadataName: an `[assembly:]` target is not a
        // declaration, and one lookup per compilation is cheaper than the syntax
        // walk would be.
        IncrementalValueProvider<ProcessorModel?> processor =
            context.CompilationProvider.Select(static (compilation, _) => ProcessorModel.From(compilation));

        context.RegisterSourceOutput(
            components.Combine(transforms).Combine(processor),
            static (production, source) =>
                Emit(production, source.Left.Left, source.Left.Right, source.Right));
    }

    private static void Emit(
        SourceProductionContext context,
        ImmutableArray<ComponentModel> components,
        ImmutableArray<TransformModel> transforms,
        ProcessorModel? processor)
    {
        bool failed = false;
        foreach (ComponentModel component in components)
        {
            foreach (Diagnostic diagnostic in component.Diagnostics)
            {
                context.ReportDiagnostic(diagnostic);
                failed = true;
            }
        }
        foreach (TransformModel transform in transforms)
        {
            foreach (Diagnostic diagnostic in transform.Diagnostics)
            {
                context.ReportDiagnostic(diagnostic);
                failed = true;
            }
        }

        if (processor is null)
        {
            // A project that only references Saci.Sdk is not a processor, and
            // generating an export for it would break its build. Only an assembly
            // that already carries the attributes is missing one.
            if (!components.IsEmpty || !transforms.IsEmpty)
            {
                context.ReportDiagnostic(Diagnostic.Create(Rules.NoProcessor, Location.None));
            }
            return;
        }
        if (components.IsEmpty)
        {
            context.ReportDiagnostic(Diagnostic.Create(Rules.NoComponent, Location.None));
            return;
        }

        // Components in name order, which is the order the host writes segments
        // in and the order the fingerprint is computed in.
        List<ComponentModel> ordered = new List<ComponentModel>(components);
        ordered.Sort(static (a, b) => string.CompareOrdinal(a.ComponentName, b.ComponentName));

        HashSet<string> names = new HashSet<string>(StringComparer.Ordinal);
        Dictionary<string, List<TransformModel>> byRow =
            new Dictionary<string, List<TransformModel>>(StringComparer.Ordinal);
        foreach (ComponentModel component in ordered)
        {
            if (!names.Add(component.ComponentName))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    Rules.ComponentDeclaredTwice, Location.None, component.ComponentName));
                failed = true;
            }
            if (component.Fields.IsEmpty)
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    Rules.ComponentHasNoFields, Location.None, component.ComponentName));
                failed = true;
            }
            byRow[component.TypeName] = new List<TransformModel>();
        }

        List<TransformModel> sorted = new List<TransformModel>(transforms);
        sorted.Sort(TransformModel.Compare);
        foreach (TransformModel transform in sorted)
        {
            if (!transform.Diagnostics.IsEmpty)
            {
                continue;
            }
            if (!byRow.TryGetValue(transform.RowTypeName, out List<TransformModel>? bound))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    Rules.TransformRowUnknown,
                    transform.Location,
                    transform.Invocation,
                    transform.RowTypeDisplay));
                failed = true;
                continue;
            }
            bound.Add(transform);
        }

        if (failed)
        {
            return;
        }
        context.AddSource(
            "SaciPipelineExports.g.cs",
            SourceText.From(Render(processor, ordered, byRow), Encoding.UTF8));
    }

    private static string Render(
        ProcessorModel processor,
        List<ComponentModel> components,
        Dictionary<string, List<TransformModel>> byRow)
    {
        string types = Names.ImportsNamespace + ".ITypesImports";
        string hostIo = Names.ImportsNamespace + ".IHostIoImports";
        string descriptors =
            "global::System.Collections.Generic.List<" + types + ".ComponentDescriptor>";

        StringBuilder code = new StringBuilder(4096);
        code.AppendLine("// <auto-generated/>");
        code.AppendLine("// Generated by Saci.Sdk.Generators from [assembly: SaciProcessor], [SaciComponent]");
        code.AppendLine("// and [SaciTransform]. Do not edit.");
        code.AppendLine("#nullable enable");
        code.AppendLine();
        code.AppendLine("namespace " + Names.ExportNamespace + ";");
        code.AppendLine();
        code.AppendLine("/// <summary>The processor's `saci:pipeline/pipeline` export. The namespace and");
        code.AppendLine("/// the class name are dictated by wit-bindgen's generated interop, which calls");
        code.AppendLine("/// both methods by unqualified name.</summary>");
        code.AppendLine("public sealed class " + Names.ExportClass + " : IPipelineExports");
        code.AppendLine("{");
        code.AppendLine("    private static readonly global::Saci.Sdk.SaciProcessorInfo Processor =");
        code.AppendLine("        new global::Saci.Sdk.SaciProcessorInfo("
            + Quote(processor.Name) + ", " + Quote(processor.Version) + ", "
            + Quote(processor.LogTarget) + ");");
        code.AppendLine();
        code.AppendLine("    private static readonly global::Saci.Sdk.SaciHostIo Host =");
        code.AppendLine("        new global::Saci.Sdk.SaciHostIo(");
        code.AppendLine("            static (level, target, message) => " + hostIo + ".Log(ToWitLevel(level), target, message),");
        code.AppendLine("            static (name, value) => " + hostIo + ".Metric(name, value),");
        code.AppendLine("            static key => " + hostIo + ".GetConfig(key));");
        code.AppendLine();
        code.AppendLine("    private static readonly global::Saci.Sdk.SaciComponentBinding[] Components =");
        code.AppendLine("        new global::Saci.Sdk.SaciComponentBinding[]");
        code.AppendLine("        {");
        foreach (ComponentModel component in components)
        {
            RenderComponent(code, component, byRow[component.TypeName]);
        }
        code.AppendLine("        };");
        code.AppendLine();
        RenderDescribe(code, processor, types, hostIo, descriptors);
        code.AppendLine();
        RenderRunBatch(code, processor, types, hostIo);
        code.AppendLine();
        RenderLevelMap(code, hostIo);
        code.AppendLine("}");
        return code.ToString();
    }

    private static void RenderComponent(
        StringBuilder code, ComponentModel component, List<TransformModel> transforms)
    {
        string row = component.TypeName;
        code.AppendLine("            new global::Saci.Sdk.SaciComponentBinding<" + row + ">(");
        code.AppendLine("                " + Quote(component.ComponentName) + ",");
        code.AppendLine("                " + component.Version.ToString(
            System.Globalization.CultureInfo.InvariantCulture) + "u,");
        code.AppendLine("                static () => new " + row + "(),");
        code.AppendLine("                new global::Saci.Sdk.SaciFieldBinding<" + row + ">[]");
        code.AppendLine("                {");
        foreach (FieldModel field in component.Fields)
        {
            code.AppendLine(
                "                    global::Saci.Sdk.SaciField." + Names.Factory(field.Kind)
                + "<" + row + ">(" + Quote(field.WireName)
                + ", static row => row." + field.PropertyName
                + ", static (row, value) => row." + field.PropertyName + " = value),");
        }
        code.AppendLine("                },");
        code.AppendLine("                new global::System.Action<" + row
            + ", global::Saci.Sdk.SaciConfig>[]");
        code.AppendLine("                {");
        foreach (TransformModel transform in transforms)
        {
            code.AppendLine("                    static (row, config) => " + transform.Invocation
                + (transform.Parameters == 1 ? "(row)," : "(row, config),"));
        }
        code.AppendLine("                }),");
    }

    private static void RenderDescribe(
        StringBuilder code, ProcessorModel processor, string types, string hostIo, string descriptors)
    {
        code.AppendLine("    /// <summary>Reports the processor's identity, its component schemas and");
        code.AppendLine("    /// the schema fingerprint the host gates compatibility on.</summary>");
        code.AppendLine("    public static " + types + ".PipelineDescriptor Describe()");
        code.AppendLine("    {");
        code.AppendLine("        try");
        code.AppendLine("        {");
        code.AppendLine("            global::Saci.Sdk.SaciDescriptor described =");
        code.AppendLine("                global::Saci.Sdk.SaciRuntime.Describe(Processor, Components);");
        code.AppendLine("            " + descriptors + " components = new " + descriptors
            + "(described.Components.Length);");
        code.AppendLine("            foreach (global::Saci.Sdk.SaciComponentSchema component in described.Components)");
        code.AppendLine("            {");
        code.AppendLine("                components.Add(new " + types
            + ".ComponentDescriptor(component.Name, component.ArrowSchemaIpc));");
        code.AppendLine("            }");
        code.AppendLine("            return new " + types + ".PipelineDescriptor(");
        code.AppendLine("                described.Name, described.Version, components,");
        code.AppendLine("                described.Stateful, described.SchemaFingerprint);");
        code.AppendLine("        }");
        code.AppendLine("        catch (global::System.Exception failure)");
        code.AppendLine("        {");
        code.AppendLine("            // describe has no error arm in the WIT world. Reporting here turns");
        code.AppendLine("            // what would be a trapped instance into a named log line plus an");
        code.AppendLine("            // empty descriptor, which the host rejects with a readable reason.");
        code.AppendLine("            " + hostIo + ".Log(" + hostIo + ".LogLevel.ERROR, "
            + Quote(processor.LogTarget) + ", " + Quote(processor.Name + ": describe: ")
            + " + failure.Message);");
        code.AppendLine("            return new " + types + ".PipelineDescriptor(");
        code.AppendLine("                " + Quote(processor.Name) + ", " + Quote(processor.Version)
            + ", new " + descriptors + "(), false, \"\");");
        code.AppendLine("        }");
        code.AppendLine("    }");
    }

    private static void RenderRunBatch(
        StringBuilder code, ProcessorModel processor, string types, string hostIo)
    {
        code.AppendLine("    /// <summary>Decodes the batch, runs every transform, and re-encodes it.</summary>");
        code.AppendLine("    public static " + types + ".RunResult RunBatch(byte[] input, byte[]? prior)");
        code.AppendLine("    {");
        code.AppendLine("        try");
        code.AppendLine("        {");
        code.AppendLine("            global::Saci.Sdk.SaciBatchResult result =");
        code.AppendLine("                global::Saci.Sdk.SaciRuntime.RunBatch(input, prior, Host, Processor, Components);");
        code.AppendLine("            return new " + types + ".RunResult(");
        code.AppendLine("                result.Output,");
        code.AppendLine("                result.Checkpoint,");
        code.AppendLine("                new " + types + ".RunMetrics(");
        code.AppendLine("                    result.WallNs, result.RowsIn, result.RowsOut,");
        code.AppendLine("                    result.SystemsRun, result.Retries),");
        code.AppendLine("                // routes null: the host multicasts the output to every");
        code.AppendLine("                // downstream link, which is what a non-routing stage wants.");
        code.AppendLine("                null);");
        code.AppendLine("        }");
        code.AppendLine("        catch (global::SaciPipelineWorld.WitException)");
        code.AppendLine("        {");
        code.AppendLine("            throw;");
        code.AppendLine("        }");
        code.AppendLine("        catch (global::System.Exception failure)");
        code.AppendLine("        {");
        code.AppendLine("            // Only WitException reaches the WIT error arm; anything else");
        code.AppendLine("            // unwinds out of the component and traps the instance, leaving the");
        code.AppendLine("            // host with no reason at all. schema-mismatch is reserved for a");
        code.AppendLine("            // future load-time check and must never come out of run-batch.");
        code.AppendLine("            string reason =");
        code.AppendLine("                failure is global::Saci.Sdk.SaciProcessorException");
        code.AppendLine("                || failure is global::Saci.ArrowIpc.ArrowIpcException");
        code.AppendLine("                    ? failure.Message");
        code.AppendLine("                    : failure.GetType().Name + \": \" + failure.Message;");
        code.AppendLine("            " + hostIo + ".Log(" + hostIo + ".LogLevel.ERROR, "
            + Quote(processor.LogTarget) + ", " + Quote(processor.Name + ": ") + " + reason);");
        code.AppendLine("            throw new global::SaciPipelineWorld.WitException<" + types
            + ".RunError>(");
        code.AppendLine("                " + types + ".RunError.Permanent(reason), 0);");
        code.AppendLine("        }");
        code.AppendLine("    }");
    }

    private static void RenderLevelMap(StringBuilder code, string hostIo)
    {
        code.AppendLine("    private static " + hostIo + ".LogLevel ToWitLevel(global::Saci.Sdk.SaciLogLevel level)");
        code.AppendLine("    {");
        code.AppendLine("        switch (level)");
        code.AppendLine("        {");
        code.AppendLine("            case global::Saci.Sdk.SaciLogLevel.Trace: return " + hostIo + ".LogLevel.TRACE;");
        code.AppendLine("            case global::Saci.Sdk.SaciLogLevel.Debug: return " + hostIo + ".LogLevel.DEBUG;");
        code.AppendLine("            case global::Saci.Sdk.SaciLogLevel.Warn: return " + hostIo + ".LogLevel.WARN;");
        code.AppendLine("            case global::Saci.Sdk.SaciLogLevel.Error: return " + hostIo + ".LogLevel.ERROR;");
        code.AppendLine("            default: return " + hostIo + ".LogLevel.INFO;");
        code.AppendLine("        }");
        code.AppendLine("    }");
    }

    /// <summary>Renders a C# string literal.</summary>
    private static string Quote(string text) =>
        "\"" + text.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
}
