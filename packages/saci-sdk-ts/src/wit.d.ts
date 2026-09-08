// Types for the WIT import specifier.
//
// Nothing on disk answers `saci:pipeline/host-io@0.3.0`: jco satisfies it from
// the WIT world at componentize time, and the stage that builds this package
// into a component is where `jco types` runs. A tsconfig `paths` entry would
// also redirect jco's bundler, which then fails on a declaration file that has
// no runtime exports, so the mapping lives here where only the type checker
// sees it.
//
// The three functions and the enum are the whole `host-io` interface of
// `crates/saci-processor/wit/pipeline.wit`. `core.ts` restates their shape as
// `HostIo`, and `index.ts` is the one place the two meet, so a drift in the WIT
// package shows up as a type error there rather than as a trap inside a
// component.

declare module 'saci:pipeline/host-io@0.3.0' {
  export type LogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error';
  export function log(level: LogLevel, target: string, message: string): void;
  export function metric(name: string, value: number): void;
  export function getConfig(key: string): string | undefined;
}
