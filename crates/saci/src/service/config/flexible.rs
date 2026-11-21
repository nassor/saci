//! Flexible scalar deserializers for the config types.

use serde::Deserialize;

// Flexible deserializers: accept either the native type, or a string that
// parses to it. Lets env-var substitution work for non-string fields
// (`id="${SACI_NODE_ID}"`).

pub(crate) fn de_u64_flexible<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum U64OrStr {
        Int(u64),
        Str(String),
    }
    match U64OrStr::deserialize(d)? {
        U64OrStr::Int(n) => Ok(n),
        U64OrStr::Str(s) => s.trim().parse().map_err(serde::de::Error::custom),
    }
}

pub(crate) fn de_bool_flexible<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrStr {
        Bool(bool),
        Str(String),
    }
    match BoolOrStr::deserialize(d)? {
        BoolOrStr::Bool(b) => Ok(b),
        BoolOrStr::Str(s) => s.trim().parse().map_err(serde::de::Error::custom),
    }
}

pub(crate) fn de_opt_u64_flexible<'de, D>(d: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum U64OrStr {
        Int(u64),
        Str(String),
    }
    match Option::<U64OrStr>::deserialize(d)? {
        None => Ok(None),
        Some(U64OrStr::Int(n)) => Ok(Some(n)),
        Some(U64OrStr::Str(s)) => s.trim().parse().map(Some).map_err(serde::de::Error::custom),
    }
}

pub(crate) fn de_opt_bool_flexible<'de, D>(d: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrStr {
        Bool(bool),
        Str(String),
    }
    match Option::<BoolOrStr>::deserialize(d)? {
        None => Ok(None),
        Some(BoolOrStr::Bool(b)) => Ok(Some(b)),
        Some(BoolOrStr::Str(s)) => s.trim().parse().map(Some).map_err(serde::de::Error::custom),
    }
}

pub(crate) fn de_opt_f64_flexible<'de, D>(d: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum F64OrStr {
        Num(f64),
        Str(String),
    }
    match Option::<F64OrStr>::deserialize(d)? {
        None => Ok(None),
        Some(F64OrStr::Num(n)) => Ok(Some(n)),
        Some(F64OrStr::Str(s)) => s.trim().parse().map(Some).map_err(serde::de::Error::custom),
    }
}
