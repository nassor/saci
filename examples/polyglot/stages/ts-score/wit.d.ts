// Types for the WIT import specifier.
//
// Nothing on disk answers `saci:pipeline/host-io@0.3.0`: jco satisfies it from
// the WIT world at componentize time. A tsconfig `paths` entry would also
// redirect the bundler, which then fails on a declaration file that has no
// runtime exports, so the mapping lives here where only the type checker sees
// it. `npm run types` regenerates the target from the same WIT package the
// build reads, so the import cannot drift from the world.

declare module 'saci:pipeline/host-io@0.3.0' {
  export { log, metric, getConfig, type LogLevel } from './types/interfaces/saci-pipeline-host-io.js';
}
