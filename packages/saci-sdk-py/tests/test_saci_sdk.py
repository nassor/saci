"""Native tests for the SACI Python SDK.

These run under plain CPython, where there is no `wit_world`: the SDK falls back
to local stand-ins for the generated records and for `host-io`, so a processor
class can be driven end to end here exactly as the host drives it inside the
component. `saci_sdk.LOCAL_HOST` is that stand-in host — the config a batch sees
and the metric and log calls its transforms made.

The wire bytes are real: every round trip goes through `saci_sdk.arrow_ipc`, the
same codec the component links.
"""

import unittest
from dataclasses import dataclass

from saci_sdk import arrow_ipc
import saci_sdk


@saci_sdk.component
@dataclass
class Row:
    """One row of every type the wire format carries."""

    id: int
    label: str
    amount: float
    valid: bool = False
    note: str = ""


@saci_sdk.component
@dataclass
class X:
    """The one-field component the fingerprint reference value names."""

    x: int


#: The polyglot example's `Order`, field for field, as the stage declares it.
#: Its fingerprint is a cross-language constant, so it is asserted here rather
#: than left to the integration test.
@saci_sdk.component
@dataclass
class Order:
    id: int
    region: str
    currency: str
    amount: float
    valid: bool = False
    usd_amount: float = 0.0
    usd_amount_display: str = ""
    risk_score: float = 0.0
    flagged: bool = False
    fee: float = 0.0
    review_tier: int = 0
    settlement: str = ""


@saci_sdk.transform(Row)
def double(row, config):
    row.amount *= config.float("factor", 2.0)


@saci_sdk.transform(Row)
def label(row, config):
    row.note = "{}:{:.1f}".format(row.label, row.amount)


@saci_sdk.batch(Row)
def report(rows, config):
    config.metric("test.total", sum(row.amount for row in rows))
    config.log("info", "test", "saw {} rows".format(len(rows)))


def _input(rows=3):
    """A wire envelope holding `rows` rows of `Row`, plus the bitmap."""
    stream = arrow_ipc.SaciStream()
    stream.write_component(
        "Row",
        1,
        arrow_ipc.Int64Column("id", list(range(1, rows + 1))),
        arrow_ipc.Utf8Column("label", ["a", "b", "c"][:rows]),
        arrow_ipc.Float64Column("amount", [1.0, 2.5, -3.0][:rows]),
        arrow_ipc.BoolColumn("valid", [True, False, True][:rows]),
        arrow_ipc.Utf8Column("note", [""] * rows),
    )
    stream.write_alive([True] * rows)
    return stream.to_bytes()


class SchemaDerivationTest(unittest.TestCase):
    """What `@saci_sdk.component` reads off a dataclass."""

    def test_fields_keep_their_names_types_and_order(self):
        spec = saci_sdk._spec(Row)
        self.assertEqual(spec.name, "Row")
        self.assertEqual(spec.version, 1)
        self.assertEqual(
            [(field.name, field.column) for field in spec.fields],
            [
                ("id", arrow_ipc.Int64Column),
                ("label", arrow_ipc.Utf8Column),
                ("amount", arrow_ipc.Float64Column),
                ("valid", arrow_ipc.BoolColumn),
                ("note", arrow_ipc.Utf8Column),
            ],
        )

    def test_the_descriptor_carries_the_derived_schema(self):
        pipeline = saci_sdk.processor("p", "0.1.0", double)()
        descriptor = pipeline.describe()
        self.assertEqual(descriptor.name, "p")
        self.assertEqual(descriptor.version, "0.1.0")
        self.assertFalse(descriptor.stateful)
        self.assertEqual(len(descriptor.components), 1)
        self.assertEqual(descriptor.components[0].name, "Row")
        # A schema-only stream: one Schema message and the end-of-stream marker.
        self.assertEqual(
            descriptor.components[0].arrow_schema_ipc,
            arrow_ipc.schema_ipc(
                arrow_ipc.Int64Column("id", ()),
                arrow_ipc.Utf8Column("label", ()),
                arrow_ipc.Float64Column("amount", ()),
                arrow_ipc.BoolColumn("valid", ()),
                arrow_ipc.Utf8Column("note", ()),
            ),
        )

    def test_a_name_and_version_override_the_defaults(self):
        @saci_sdk.component(name="Renamed", version=7)
        @dataclass
        class Local:
            a: int

        spec = saci_sdk._spec(Local)
        self.assertEqual((spec.name, spec.version), ("Renamed", 7))

    def test_an_unsupported_annotation_is_refused_at_import_time(self):
        with self.assertRaises(TypeError):

            @saci_sdk.component
            @dataclass
            class Bad:
                when: bytes

    def test_a_plain_class_is_refused(self):
        with self.assertRaises(TypeError):

            @saci_sdk.component
            class NotADataclass:
                pass

    def test_an_undecorated_class_is_not_a_component(self):
        @dataclass
        class Loose:
            a: int

        with self.assertRaises(ValueError):
            saci_sdk.transform(Loose)


class FingerprintTest(unittest.TestCase):
    """The value the host recomputes and gates the load on."""

    def test_known_values(self):
        self.assertEqual(saci_sdk.fingerprint(X), "43623dda")
        self.assertEqual(saci_sdk.fingerprint(Order), "f6405a7b")

    def test_it_is_eight_lowercase_hex_digits(self):
        value = saci_sdk.fingerprint(Row)
        self.assertEqual(len(value), 8)
        self.assertEqual(value, value.lower())
        int(value, 16)

    def test_adding_a_field_changes_it(self):
        @saci_sdk.component(name="X")
        @dataclass
        class Wider:
            x: int
            y: int

        self.assertNotEqual(saci_sdk.fingerprint(Wider), saci_sdk.fingerprint(X))

    def test_retyping_a_field_does_not(self):
        @saci_sdk.component(name="X")
        @dataclass
        class Retyped:
            x: float

        self.assertEqual(saci_sdk.fingerprint(Retyped), saci_sdk.fingerprint(X))

    def test_the_version_is_part_of_it(self):
        @saci_sdk.component(name="X", version=2)
        @dataclass
        class Versioned:
            x: int

        self.assertNotEqual(saci_sdk.fingerprint(Versioned), saci_sdk.fingerprint(X))


class RunBatchTest(unittest.TestCase):
    """A processor driven exactly as the host drives it."""

    def setUp(self):
        saci_sdk.LOCAL_HOST.reset()

    def test_transforms_run_in_order_and_the_output_round_trips(self):
        pipeline = saci_sdk.processor("p", "0.1.0", double, label, report)()
        result = pipeline.run_batch(_input(), None)

        batch = arrow_ipc.SaciStream(result.output).component("Row")
        self.assertEqual(batch.rows, 3)
        self.assertEqual(batch.int64s("id"), [1, 2, 3])
        self.assertEqual(batch.float64s("amount"), [2.0, 5.0, -6.0])
        self.assertEqual(batch.bools("valid"), [True, False, True])
        # `label` ran after `double`, so it saw the doubled amount.
        self.assertEqual(batch.strings("note"), ["a:2.0", "b:5.0", "c:-6.0"])

    def test_registration_order_decides_the_result(self):
        pipeline = saci_sdk.processor("p", "0.1.0", label, double)()
        result = pipeline.run_batch(_input(), None)

        batch = arrow_ipc.SaciStream(result.output).component("Row")
        # `label` ran first this time, so it saw the original amounts.
        self.assertEqual(batch.strings("note"), ["a:1.0", "b:2.5", "c:-3.0"])
        self.assertEqual(batch.float64s("amount"), [2.0, 5.0, -6.0])

    def test_config_values_reach_the_transform(self):
        saci_sdk.LOCAL_HOST.config["factor"] = "10"
        pipeline = saci_sdk.processor("p", "0.1.0", double)()
        result = pipeline.run_batch(_input(), None)

        batch = arrow_ipc.SaciStream(result.output).component("Row")
        self.assertEqual(batch.float64s("amount"), [10.0, 25.0, -30.0])

    def test_a_batch_transform_runs_once_with_every_row(self):
        pipeline = saci_sdk.processor("p", "0.1.0", double, report)()
        pipeline.run_batch(_input(), None)

        self.assertEqual(saci_sdk.LOCAL_HOST.metrics, [("test.total", 1.0)])
        self.assertEqual(
            saci_sdk.LOCAL_HOST.logs,
            [(saci_sdk.LogLevel.INFO, "test", "saw 3 rows")],
        )

    def test_metrics_report_the_rows_and_a_real_clock(self):
        pipeline = saci_sdk.processor("p", "0.1.0", double, report)()
        metrics = pipeline.run_batch(_input(), None).metrics

        self.assertEqual((metrics.rows_in, metrics.rows_out), (3, 3))
        self.assertEqual((metrics.systems_run, metrics.retries), (2, 0))
        self.assertGreater(metrics.wall_ns, 0)

    def test_it_is_stateless(self):
        pipeline = saci_sdk.processor("p", "0.1.0", double)()
        result = pipeline.run_batch(_input(), b"a prior checkpoint")
        self.assertIsNone(result.checkpoint)
        self.assertIsNone(result.routes)

    def test_an_empty_batch_round_trips(self):
        pipeline = saci_sdk.processor("p", "0.1.0", double, report)()
        result = pipeline.run_batch(_input(rows=0), None)

        batch = arrow_ipc.SaciStream(result.output).component("Row")
        self.assertEqual(batch.rows, 0)
        self.assertEqual(batch.strings("label"), [])

    def test_segments_it_does_not_own_pass_through_byte_identical(self):
        stream = arrow_ipc.SaciStream(_input())
        stream.write_component("Other", 3, arrow_ipc.Int64Column("n", [7, 8, 9]))
        raw = stream.to_bytes()

        pipeline = saci_sdk.processor("p", "0.1.0", double)()
        output = pipeline.run_batch(raw, None).output

        before = arrow_ipc.SaciStream(raw)
        after = arrow_ipc.SaciStream(output)
        self.assertEqual(after.component_names, ["Other", "Row", "__alive"])
        self.assertEqual(after.component("Other").int64s("n"), [7, 8, 9])
        self.assertEqual(
            after.component("__alive").bools("alive"),
            before.component("__alive").bools("alive"),
        )

    def test_a_utf8_column_can_be_written(self):
        """The whole reason a processor re-encodes instead of patching."""
        pipeline = saci_sdk.processor("p", "0.1.0", label)()
        result = pipeline.run_batch(_input(), None)

        batch = arrow_ipc.SaciStream(result.output).component("Row")
        self.assertEqual(batch.strings("note"), ["a:1.0", "b:2.5", "c:-3.0"])


class ErrorMappingTest(unittest.TestCase):
    """Everything that goes wrong inside `run-batch` is one permanent error."""

    def setUp(self):
        saci_sdk.LOCAL_HOST.reset()
        self.pipeline = saci_sdk.processor("p", "0.1.0", double)()

    def error(self, *args):
        with self.assertRaises(saci_sdk.Err) as caught:
            self.pipeline.run_batch(*args)
        return caught.exception.value

    def test_malformed_input_is_permanent(self):
        value = self.error(b"not a stream", None)
        self.assertIsInstance(value, saci_sdk.types.RunError_Permanent)
        self.assertIn("p: ", value.value)

    def test_a_missing_component_is_permanent(self):
        stream = arrow_ipc.SaciStream()
        stream.write_component("Elsewhere", 1, arrow_ipc.Int64Column("n", [1]))
        stream.write_alive([True])

        value = self.error(stream.to_bytes(), None)
        self.assertIn("no segment for component 'Row'", value.value)

    def test_unparseable_config_is_permanent(self):
        saci_sdk.LOCAL_HOST.config["factor"] = "one point one"
        value = self.error(_input(), None)
        self.assertIn("is not a number", value.value)

    def test_a_transform_raising_is_permanent_and_named(self):
        @saci_sdk.transform(Row)
        def explode(row, config):
            raise KeyError("boom")

        pipeline = saci_sdk.processor("p", "0.1.0", explode)()
        with self.assertRaises(saci_sdk.Err) as caught:
            pipeline.run_batch(_input(), None)
        self.assertIn("unexpected KeyError", caught.exception.value.value)


class ProcessorShapeTest(unittest.TestCase):
    """What `processor` accepts, and what it is."""

    def setUp(self):
        saci_sdk.LOCAL_HOST.reset()

    def test_it_subclasses_the_generated_export(self):
        pipeline = saci_sdk.processor("p", "0.1.0", double)
        self.assertTrue(issubclass(pipeline, saci_sdk.exports.Pipeline))

    def test_an_undecorated_function_is_refused(self):
        def loose(row, config):
            pass

        with self.assertRaises(ValueError):
            saci_sdk.processor("p", "0.1.0", loose)

    def test_at_least_one_transform_is_required(self):
        with self.assertRaises(ValueError):
            saci_sdk.processor("p", "0.1.0")

    def test_two_components_are_both_processed(self):
        @saci_sdk.component
        @dataclass
        class Second:
            n: int

        @saci_sdk.transform(Second)
        def bump(row, config):
            row.n += 1

        stream = arrow_ipc.SaciStream(_input())
        stream.write_component("Second", 1, arrow_ipc.Int64Column("n", [1, 2, 3]))

        pipeline = saci_sdk.processor("p", "0.1.0", double, bump)()
        result = pipeline.run_batch(stream.to_bytes(), None)

        parsed = arrow_ipc.SaciStream(result.output)
        self.assertEqual(parsed.component("Second").int64s("n"), [2, 3, 4])
        self.assertEqual(parsed.component("Row").float64s("amount"), [2.0, 5.0, -6.0])
        self.assertEqual(result.metrics.rows_in, 6)
        self.assertEqual(len(pipeline.describe().components), 2)


class SaciConfigTest(unittest.TestCase):
    """The one host object a transform touches."""

    def setUp(self):
        saci_sdk.LOCAL_HOST.reset()
        self.config = saci_sdk.SaciConfig()

    def test_absent_keys_fall_back_to_the_default(self):
        self.assertEqual(self.config.float("nothing", 1.25), 1.25)

    def test_a_present_value_wins(self):
        saci_sdk.LOCAL_HOST.config["rate"] = "0.0068"
        self.assertEqual(self.config.float("rate", 1.0), 0.0068)

    def test_an_unparseable_value_raises(self):
        saci_sdk.LOCAL_HOST.config["rate"] = "cheap"
        with self.assertRaises(ValueError):
            self.config.float("rate", 1.0)

    def test_a_key_is_read_from_the_host_once(self):
        calls = []
        original = saci_sdk._get_config

        def counting(key):
            calls.append(key)
            return original(key)

        saci_sdk._get_config = counting
        try:
            for _ in range(5):
                self.config.float("rate", 1.0)
        finally:
            saci_sdk._get_config = original
        self.assertEqual(calls, ["rate"])

    def test_log_levels_are_names(self):
        for level in ("trace", "debug", "info", "warn", "error"):
            with self.subTest(level=level):
                self.config.log(level, "t", "m")
        self.assertEqual(len(saci_sdk.LOCAL_HOST.logs), 5)
        with self.assertRaises(ValueError):
            self.config.log("critical", "t", "m")

    def test_metrics_pass_straight_through(self):
        self.config.metric("a.b", 2)
        self.assertEqual(saci_sdk.LOCAL_HOST.metrics, [("a.b", 2.0)])


if __name__ == "__main__":
    unittest.main()
