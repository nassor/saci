"""SACI processors in Python: one dataclass per component, one function per transform.

A processor is a `@dataclass` naming the component's columns and a function per
transform. The decorators do the rest, and they do it while componentize-py
builds the component: that build imports the module once and snapshots the
CPython heap, so a schema derived at import is a schema baked into the wasm
file rather than work the hot path repeats.

    from dataclasses import dataclass
    import saci_sdk

    @saci_sdk.component
    @dataclass
    class Order:
        id: int
        amount: float

    @saci_sdk.transform(Order)
    def convert(row, config):
        row.amount *= config.float("rate", 1.0)

    Pipeline = saci_sdk.processor("converter", "0.1.0", convert)

`Pipeline` is the module-level name componentize-py looks up for the
`saci:pipeline/pipeline` export, and the class bound to it subclasses the
generated `wit_world.exports.Pipeline` exactly as a hand-written processor
would. Nothing else about the WIT world reaches processor code: no bindings
import, no descriptor record, no `Err` variant, no Arrow.

Unlike an in-place processor, one built here re-encodes its component from the
row objects, so it may write a `Utf8` column and may return fewer rows than it
was handed. Every segment it does not own passes through byte-identical.

Every import below is at module level, because componentize-py resolves imports
when it builds and a function-local import is an `ImportError` inside the
finished component. The WIT bindings are the one import that can be absent:
under plain CPython, where this package's tests run, there is no `wit_world`,
so the records and host calls are defined locally instead. That branch is never
taken inside a component.
"""

import dataclasses
import time
import typing
from . import arrow_ipc

try:
    from componentize_py_types import Err
    from wit_world import exports
    from wit_world.imports import types
    from wit_world.imports.host_io import (
        LogLevel,
        get_config as _get_config,
        log as _log,
        metric as _metric,
    )

    #: True inside a component, where the WIT bindings exist.
    HOSTED = True
except ImportError:
    HOSTED = False

    @dataclasses.dataclass(frozen=True)
    class Err(Exception):
        """Stand-in for `componentize_py_types.Err`: a `result`'s error arm."""

        value: object

    class exports:
        """Stand-in for `wit_world.exports`."""

        class Pipeline:
            """Stand-in base for the `saci:pipeline/pipeline` export."""

    class types:
        """Stand-ins for the records in `wit_world.imports.types`."""

        @dataclasses.dataclass
        class ComponentDescriptor:
            name: str
            arrow_schema_ipc: bytes

        @dataclasses.dataclass
        class PipelineDescriptor:
            name: str
            version: str
            components: list
            stateful: bool
            schema_fingerprint: str

        @dataclasses.dataclass
        class RunMetrics:
            wall_ns: int
            rows_in: int
            rows_out: int
            systems_run: int
            retries: int

        @dataclasses.dataclass
        class RunResult:
            output: bytes
            checkpoint: object
            metrics: object
            routes: object

        @dataclasses.dataclass
        class RunError_Permanent:  # the name componentize-py generates
            value: str

    class LogLevel:
        """Stand-in for the generated `host-io` log level enum."""

        TRACE = "trace"
        DEBUG = "debug"
        INFO = "info"
        WARN = "warn"
        ERROR = "error"

    class _LocalHost:
        """Stand-in for `host-io` itself: what a test injects and observes.

        Exists only off-wasm. `config` is what `get-config` answers with, and
        `metrics` and `logs` record the calls a transform made.
        """

        __slots__ = ("config", "metrics", "logs")

        def __init__(self):
            self.config = {}
            self.metrics = []
            self.logs = []

        def reset(self):
            self.config.clear()
            del self.metrics[:]
            del self.logs[:]

    LOCAL_HOST = _LocalHost()

    def _get_config(key):
        return LOCAL_HOST.config.get(key)

    def _log(level, target, message):
        LOCAL_HOST.logs.append((level, target, message))

    def _metric(name, value):
        LOCAL_HOST.metrics.append((name, value))


#: Python annotation -> (the `Batch` reader that decodes such a column, the
#: codec column that encodes it). These four are the whole SACI wire format: a
#: component is columns of scalars, never nested objects.
_ARROW_TYPES = {
    int: ("int64s", arrow_ipc.Int64Column),
    float: ("float64s", arrow_ipc.Float64Column),
    bool: ("bools", arrow_ipc.BoolColumn),
    str: ("strings", arrow_ipc.Utf8Column),
}

#: Log level names a transform may pass, mapped to the generated enum so that
#: processor code never imports the bindings for one argument.
_LEVELS = {
    "trace": LogLevel.TRACE,
    "debug": LogLevel.DEBUG,
    "info": LogLevel.INFO,
    "warn": LogLevel.WARN,
    "error": LogLevel.ERROR,
}

#: FNV-1a 32 parameters, from `docs/content/library/reference/wire-format.md`.
_FNV_OFFSET = 2166136261
_FNV_PRIME = 16777619

#: Attribute the decorators leave on a transform: `(component spec, kind)`.
_TARGET = "_saci_target"
_ROW = "row"
_BATCH = "batch"

#: Registered component classes. Keyed by the exact class, so a subclass of a
#: component is not silently one too.
_REGISTRY = {}


class _FieldSpec:
    """One column: its wire name, and how it moves in and out of Arrow."""

    __slots__ = ("name", "reader", "column")

    def __init__(self, name, reader, column):
        self.name = name
        self.reader = reader
        self.column = column


class _ComponentSpec:
    """A registered dataclass: what its rows are on the wire."""

    __slots__ = ("cls", "name", "version", "fields")

    def __init__(self, cls, name, version, fields):
        self.cls = cls
        self.name = name
        self.version = version
        self.fields = fields


# --------------------------------------------------------------------------
# Registration, all of it at import time.
# --------------------------------------------------------------------------


def component(cls=None, *, name=None, version=1):
    """Register a `@dataclass` as a SACI component.

    The wire name is the class name and the column names are the field names,
    both verbatim: a component is a cross-language contract, so nothing here
    renames anything. `int`, `float`, `bool` and `str` annotations become
    `Int64`, `Float64`, `Boolean` and `Utf8` columns in declaration order,
    which is the order the schema, the buffer walk and the fingerprint all
    depend on.

    `name` overrides the wire name and `version` the schema version the segment
    stamps into `__saci_schema_version`. Both belong to the contract rather than
    to the Python class, which is why they are arguments and not conventions.

    Usable bare or called:

        @saci_sdk.component
        @dataclass
        class Order: ...

        @saci_sdk.component(name="Order", version=2)
        @dataclass
        class OrderV2: ...
    """

    def register(target):
        _REGISTRY[target] = _derive(target, name, version)
        return target

    return register if cls is None else register(cls)


def transform(target):
    """Register a function as a per-row transform over `target`.

    Called as `fn(row, config)` once per row, with a mutable instance of the
    dataclass; whatever it leaves on the row is what gets encoded. Transforms
    run in the order `processor` lists them, row by row.
    """
    return _register_transform(_spec(target), _ROW)


def batch(target):
    """Register a function as a per-batch transform over `target`.

    Called as `fn(rows, config)` once, after every per-row transform has seen
    every row, with the list of rows. A per-batch metric or log line belongs
    here: `run-batch` is the unit the host measures, so one summed observation
    is one observation, not one per row.
    """
    return _register_transform(_spec(target), _BATCH)


def fingerprint(*components):
    """The `pipeline-descriptor.schema-fingerprint` for these components.

    FNV-1a 32 over each component's name, its version as four little-endian
    bytes, and its field names in declaration order, components sorted by name.
    Names only: adding a field changes the value, retyping one does not. The
    host recomputes it from its own registry at load time and refuses a
    processor that disagrees.
    """
    value = _FNV_OFFSET
    specs = sorted((_spec(c) for c in components), key=lambda spec: spec.name)
    for spec in specs:
        chunks = [spec.name.encode("utf-8"), spec.version.to_bytes(4, "little")]
        chunks.extend(field.name.encode("utf-8") for field in spec.fields)
        for chunk in chunks:
            for byte in chunk:
                value = ((value ^ byte) * _FNV_PRIME) & 0xFFFFFFFF
    return "{:08x}".format(value)


def _register_transform(spec, kind):
    def decorate(fn):
        # The function is returned unchanged: a transform stays an ordinary
        # callable, so a test can call it with a row and a config directly.
        setattr(fn, _TARGET, (spec, kind))
        return fn

    return decorate


def _spec(target):
    """The registration `@saci_sdk.component` left on a class."""
    spec = _REGISTRY.get(target)
    if spec is None:
        raise ValueError(
            "{} is not a SACI component; decorate it with "
            "@saci_sdk.component".format(getattr(target, "__name__", target))
        )
    return spec


def _derive(cls, name, version):
    """Turn a dataclass into an ordered column list, or refuse to.

    Every refusal is raised while the module is imported, which is while
    componentize-py builds: a component that would encode the wrong schema
    fails the build instead of the batch.
    """
    if not isinstance(cls, type) or not dataclasses.is_dataclass(cls):
        raise TypeError("@saci_sdk.component needs a @dataclass; {!r} is not one".format(cls))
    if isinstance(version, bool) or not isinstance(version, int) or not 0 <= version < 2**32:
        raise ValueError("component version {!r} is not a u32".format(version))

    # Annotations may still be strings, under `from __future__ import
    # annotations` or a self-referencing module, and this is what resolves them.
    hints = typing.get_type_hints(cls)
    fields = []
    for field in dataclasses.fields(cls):
        if not field.init:
            raise TypeError(
                "{}.{} is init=False, and every column is a constructor "
                "argument".format(cls.__name__, field.name)
            )
        annotation = hints.get(field.name)
        arrow = _ARROW_TYPES.get(annotation)
        if arrow is None:
            raise TypeError(
                "{}.{} is annotated {!r}; a component field is int, float, bool "
                "or str".format(cls.__name__, field.name, annotation)
            )
        reader, column = arrow
        fields.append(_FieldSpec(field.name, reader, column))
    if not fields:
        raise TypeError("{} declares no fields, so it declares no columns".format(cls.__name__))
    return _ComponentSpec(cls, name or cls.__name__, version, tuple(fields))


# --------------------------------------------------------------------------
# The host, as a transform sees it.
# --------------------------------------------------------------------------


class SaciConfig:
    """Host config, metrics and log lines, for the duration of one batch.

    `float` caches by key: `get-config` is a host call, the WIT contract calls
    the value static, and a per-row transform would otherwise make one call per
    row for an answer that cannot change.
    """

    __slots__ = ("_raw",)

    def __init__(self):
        self._raw = {}

    def float(self, key, default):
        """`key` as a float, or `default` when the host injected no value.

        A value that will not parse is a `ValueError`: a processor handed
        `fx_eur = "one point one"` cannot guess, and the host's answer to a
        permanent error is to surface it rather than replay the batch.
        """
        try:
            raw = self._raw[key]
        except KeyError:
            raw = self._raw[key] = _get_config(key)
        if raw is None:
            return default
        try:
            return float(raw)
        except ValueError:
            raise ValueError("config {!r} is not a number: {!r}".format(key, raw)) from None

    def metric(self, name, value):
        """Observe `value` under `name`.

        The host records it as `saci_processor_metric{metric="<name>"}`, one
        observation per call, so a per-batch total belongs in a
        `@saci_sdk.batch` transform.
        """
        _metric(name, float(value))

    def log(self, level, target, message):
        """One structured log line, bridged to the host's tracing.

        `level` is a name: `trace`, `debug`, `info`, `warn` or `error`.
        """
        try:
            resolved = _LEVELS[level]
        except (KeyError, TypeError):
            raise ValueError(
                "log level {!r} is not one of {}".format(level, ", ".join(_LEVELS))
            ) from None
        _log(resolved, target, message)


# --------------------------------------------------------------------------
# The export.
# --------------------------------------------------------------------------


def processor(name, version, *transforms):
    """The `saci:pipeline/pipeline` export class for these transforms.

    Bind the result to a module-level `Pipeline`: that is the name
    componentize-py looks up, and the class returned subclasses the generated
    export, so the trampoline sees exactly what a hand-written processor gives
    it.

    Transforms are grouped by the component they were registered against, in
    first-appearance order, and run in the order given within each group. The
    descriptor, the schema bytes and the fingerprint are built here, at import
    time, which is componentize-py's pre-initialization pass: the finished
    component starts with all of it in memory.

    The processor is stateless. It reports no checkpoint and no routes, because
    a transform over rows has no state to carry and no branch to choose.
    """
    if not transforms:
        raise ValueError("a processor needs at least one transform")

    plan = []
    index = {}
    for fn in transforms:
        target = getattr(fn, _TARGET, None)
        if target is None:
            raise ValueError(
                "{} is not a transform; decorate it with @saci_sdk.transform or "
                "@saci_sdk.batch".format(getattr(fn, "__name__", fn))
            )
        spec, kind = target
        entry = index.get(spec.name)
        if entry is None:
            entry = index[spec.name] = (spec, [], [])
            plan.append(entry)
        elif entry[0] is not spec:
            raise ValueError(
                "two components are both named {!r}; a processor declares each "
                "name once".format(spec.name)
            )
        entry[1 if kind == _ROW else 2].append(fn)
    plan = tuple((spec, tuple(rows), tuple(batches)) for spec, rows, batches in plan)

    descriptor = types.PipelineDescriptor(
        name=name,
        version=version,
        components=[
            types.ComponentDescriptor(
                name=spec.name,
                arrow_schema_ipc=arrow_ipc.schema_ipc(
                    *[field.column(field.name, ()) for field in spec.fields]
                ),
            )
            for spec, _rows, _batches in plan
        ],
        stateful=False,
        schema_fingerprint=fingerprint(*[spec.cls for spec, _rows, _batches in plan]),
    )
    systems_run = len(transforms)

    class Pipeline(exports.Pipeline):
        """The `saci:pipeline/pipeline` export: decode, transform, re-encode."""

        def describe(self):
            return descriptor

        def run_batch(self, input, prior):
            started = time.monotonic_ns()
            try:
                stream = arrow_ipc.SaciStream(input)
                config = SaciConfig()
                rows_total = 0
                for spec, row_transforms, batch_transforms in plan:
                    rows = _decode(spec, stream.component(spec.name))
                    for row in rows:
                        for fn in row_transforms:
                            fn(row, config)
                    for fn in batch_transforms:
                        fn(rows, config)
                    stream.write_component(spec.name, spec.version, *_encode(spec, rows))
                    rows_total += len(rows)
                return types.RunResult(
                    # Every segment this processor does not own, the liveness
                    # bitmap included, comes straight out of the input.
                    output=stream.to_bytes(),
                    # Stateless: `prior` is ignored and the host stores nothing.
                    checkpoint=None,
                    metrics=types.RunMetrics(
                        wall_ns=time.monotonic_ns() - started,
                        rows_in=rows_total,
                        rows_out=rows_total,
                        systems_run=systems_run,
                        retries=0,
                    ),
                    # `routes` none: the host multicasts to every downstream link.
                    routes=None,
                )
            except ValueError as exc:
                # Malformed input, unusable config, a value no column can hold:
                # replaying the batch cannot help.
                raise Err(types.RunError_Permanent("{}: {}".format(name, exc))) from exc
            except Exception as exc:
                # The WIT variant has no "unknown" arm, and `run-batch` must
                # never emit `schema-mismatch`, so everything else lands here.
                raise Err(
                    types.RunError_Permanent(
                        "{}: unexpected {}: {}".format(name, type(exc).__name__, exc)
                    )
                ) from exc

    return Pipeline


def _decode(spec, batch_view):
    """The batch's rows, as instances of the component's dataclass."""
    columns = [getattr(batch_view, field.reader)(field.name) for field in spec.fields]
    return [spec.cls(*values) for values in zip(*columns)]


def _encode(spec, rows):
    """The component's columns, read back off the rows."""
    return [
        field.column(field.name, [getattr(row, field.name) for row in rows])
        for field in spec.fields
    ]
