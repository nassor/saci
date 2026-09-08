// Reads the static config the host injected via the `config` node inside the
// service config's `wasm` node.
//
// The WIT contract hands config over as strings and leaves numeric parsing to the
// processor, so an unparseable value is a misconfiguration worth failing the
// batch on rather than silently defaulting: the same bytes and the same config
// would fail again, which is what `run-error::permanent` means.
//
// Lookups are memoised. A transform runs per row and reads its thresholds every
// time, and each miss is a call across the component boundary.

using System.Globalization;

namespace Saci.Sdk;

/// <summary>One batch's view of the host's config map.</summary>
public sealed class SaciConfig
{
    private readonly Func<string, string?> _lookup;
    private readonly Dictionary<string, string?> _seen = new(StringComparer.Ordinal);

    public SaciConfig(Func<string, string?> lookup)
    {
        ArgumentNullException.ThrowIfNull(lookup);
        _lookup = lookup;
    }

    /// <summary>The trimmed value, or null when the key is absent or blank.</summary>
    public string? GetString(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        if (_seen.TryGetValue(key, out string? cached))
        {
            return cached;
        }
        string? raw = _lookup(key);
        string? value = string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
        _seen[key] = value;
        return value;
    }

    /// <summary>Parses a float64 value, or returns <paramref name="fallback"/>.</summary>
    /// <exception cref="SaciProcessorException">The value is present but not a
    /// float64.</exception>
    public double GetDouble(string key, double fallback)
    {
        string? text = GetString(key);
        if (text is null)
        {
            return fallback;
        }
        return double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double value)
            ? value
            : throw Invalid(key, text, "a float64");
    }

    /// <summary>Parses an int64 value, or returns <paramref name="fallback"/>.</summary>
    /// <exception cref="SaciProcessorException">The value is present but not an
    /// int64.</exception>
    public long GetInt64(string key, long fallback)
    {
        string? text = GetString(key);
        if (text is null)
        {
            return fallback;
        }
        return long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value)
            ? value
            : throw Invalid(key, text, "an int64");
    }

    /// <summary>Parses `true`, `false`, `1` or `0`, or returns
    /// <paramref name="fallback"/>.</summary>
    /// <exception cref="SaciProcessorException">The value is present but not a
    /// boolean.</exception>
    public bool GetBool(string key, bool fallback)
    {
        string? text = GetString(key);
        return text switch
        {
            null => fallback,
            "1" => true,
            "0" => false,
            _ when text.Equals("true", StringComparison.OrdinalIgnoreCase) => true,
            _ when text.Equals("false", StringComparison.OrdinalIgnoreCase) => false,
            _ => throw Invalid(key, text, "a boolean"),
        };
    }

    private static SaciProcessorException Invalid(string key, string text, string want) =>
        new($"config {key}=\"{text}\" is not {want}");
}
