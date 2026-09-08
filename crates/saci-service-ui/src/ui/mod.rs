//! shadcn/ui, ported to Leptos.
//!
//! shadcn ships copy-in React source rather than an npm runtime, so "use
//! shadcn" here means: reproduce its CSS variable contract and its class
//! recipes, with the `docs/` palette's own values substituted for shadcn's
//! default neutral scale. No maintained Leptos port exists to depend on
//! instead — `RustForWeb/shadcn-ui` is archived, and
//! `cloud-shuttle/leptos-shadcn-ui` stopped publishing to crates.io while its
//! repository kept moving — so these are hand ports of the current
//! `new-york-v4` markup.
//!
//! Each component sets `data-slot` on its root, matching shadcn's own
//! convention, because several of the ported class strings target siblings and
//! children through it (for example `Card`'s
//! `has-data-[slot=card-action]:grid-cols-[1fr_auto]`).
//!
//! Class strings are written as whole literals. Tailwind v4 scans `.rs` files
//! as plain text and only sees complete string literals, so a `format!`-built
//! class name would silently produce no CSS.

mod badge;
mod button;
mod card;
mod input;
mod scroll_area;
mod select;
mod separator;
mod sheet;
mod table;
mod tabs;
mod toggle_group;
mod tooltip;

pub use badge::{Badge, BadgeTone};
pub use button::{Button, ButtonTone};
pub use card::{Card, CardContent, CardHeader, CardTitle};
pub use input::Input;
pub use scroll_area::ScrollArea;
pub use select::Select;
pub use separator::Separator;
pub use sheet::Sheet;
pub use table::{Table, TableBody, TableCell, TableHead, TableHeader, TableRow};
pub use tabs::{TabBar, TabButton};
pub use toggle_group::{ToggleGroup, ToggleItem};
pub use tooltip::Tooltip;
