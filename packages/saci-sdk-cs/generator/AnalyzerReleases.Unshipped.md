; Analyzer release tracking for Saci.Sdk.Generators, the file RS2008 asks for.
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
SACI0001 | Saci.Sdk | Error | Assembly declares no SACI processor
SACI0002 | Saci.Sdk | Error | SACI processor declares no component
SACI0003 | Saci.Sdk | Error | SACI component must be a reference type
SACI0004 | Saci.Sdk | Error | SACI component needs a parameterless constructor
SACI0005 | Saci.Sdk | Error | SACI component property must be settable
SACI0006 | Saci.Sdk | Error | SACI component property has no Arrow type
SACI0007 | Saci.Sdk | Error | SACI component name is declared twice
SACI0008 | Saci.Sdk | Error | SACI transform has the wrong signature
SACI0009 | Saci.Sdk | Error | SACI transform names a type that is not a component
SACI0010 | Saci.Sdk | Error | SACI component declares no fields
