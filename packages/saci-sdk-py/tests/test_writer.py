"""Native tests for the Arrow IPC writer.

Every case here encodes a stream and reads it back with this package's own
reader, which is the strictest reader in the repo: it validates the framing, both
flatbuffers, the buffer table against the schema's slot count, and every span
against the declared body length. A writer mistake therefore surfaces as a
`ValueError` from the parse rather than as a wrong value.

Two cases go further and compare against `examples/polyglot/generated/`, which
arrow-rs produced: the buffer lengths and field nodes of a re-encoded fixture,
and the field list of a schema-only descriptor stream. They are skipped when the
generated directory is absent, exactly as the fixture tests are.
"""

import math
import os
import struct
import unittest

from saci_sdk import arrow_ipc

_HERE = os.path.dirname(os.path.abspath(__file__))
_GENERATED = os.path.normpath(
    os.path.join(_HERE, "..", "..", "..", "examples", "polyglot", "generated")
)
_FIXTURE_SACI = os.path.join(_GENERATED, "fixture_input.saci")
_ORDER_SCHEMA = os.path.join(_GENERATED, "order_schema.ipc")

_MISSING = (
    "examples/polyglot/generated is gitignored; run `cargo run -p saci-service "
    "--features wasm --example polyglot_schema_emit -- emit`"
)

#: Every byte of this value's IEEE-754 encoding is non-zero, so a short or
#: byte-swapped write cannot round-trip it.
_ALL_BYTES_SET = float.fromhex("0x1.1111111111111p+11")

#: The `Order` field order the polyglot example pins, which is also the order its
#: schema fingerprint and every codec's buffer walk depend on.
_ORDER_FIELDS = (
    ("id", arrow_ipc.TYPE_INT),
    ("region", arrow_ipc.TYPE_UTF8),
    ("currency", arrow_ipc.TYPE_UTF8),
    ("amount", arrow_ipc.TYPE_FLOAT),
    ("valid", arrow_ipc.TYPE_BOOL),
    ("usd_amount", arrow_ipc.TYPE_FLOAT),
    ("usd_amount_display", arrow_ipc.TYPE_UTF8),
    ("risk_score", arrow_ipc.TYPE_FLOAT),
    ("flagged", arrow_ipc.TYPE_BOOL),
    ("fee", arrow_ipc.TYPE_FLOAT),
    ("review_tier", arrow_ipc.TYPE_INT),
    ("settlement", arrow_ipc.TYPE_UTF8),
)


def _schema_fields(stream):
    """`[(field name, type id)]` of a schema-only Arrow IPC stream.

    Reaches for the module's own flatbuffer walker: a processor never *reads* a
    schema message, so the reader has no public entry point for one, and these
    tests still need to see what `schema_ipc` wrote.
    """
    buf = bytearray(stream)
    for header_type, header, _body_start, _body_length in arrow_ipc._iter_messages(
        buf, 0, len(buf)
    ):
        if header_type == arrow_ipc._HEADER_SCHEMA:
            _metadata, fields = arrow_ipc._parse_schema(header)
            return fields
    raise AssertionError("stream carries no schema message")


def _built(rows=3):
    """A two-component stream covering all four column types."""
    stream = arrow_ipc.SaciStream()
    stream.write_component(
        "Order",
        1,
        arrow_ipc.Int64Column("id", list(range(1, rows + 1))),
        arrow_ipc.Utf8Column("region", ["emea", "apac", "amer"][:rows]),
        arrow_ipc.Float64Column("amount", [1.5, -2.25, _ALL_BYTES_SET][:rows]),
        arrow_ipc.BoolColumn("valid", [True, False, True][:rows]),
    )
    stream.write_component(
        "Ledger",
        2,
        arrow_ipc.Int64Column("seq", [10] * rows),
    )
    stream.write_alive([True] * rows)
    return stream


class WriterRoundTripTest(unittest.TestCase):
    """What was written is what the reader sees."""

    def test_every_type_round_trips_exactly(self):
        raw = _built().to_bytes()
        stream = arrow_ipc.SaciStream(raw)
        self.assertEqual(stream.component_names, ["Ledger", "Order", "__alive"])

        batch = stream.component("Order")
        self.assertEqual(batch.rows, 3)
        self.assertEqual(batch.field_names, ["id", "region", "amount", "valid"])
        self.assertEqual(batch.int64s("id"), [1, 2, 3])
        self.assertEqual(batch.strings("region"), ["emea", "apac", "amer"])
        self.assertEqual(batch.float64s("amount"), [1.5, -2.25, _ALL_BYTES_SET])
        self.assertEqual(batch.bools("valid"), [True, False, True])
        self.assertEqual(stream.component("Ledger").int64s("seq"), [10, 10, 10])
        self.assertEqual(stream.component("__alive").bools("alive"), [True] * 3)

    def test_written_stream_reparses_to_the_same_bytes(self):
        raw = _built().to_bytes()
        self.assertEqual(arrow_ipc.SaciStream(raw).to_bytes(), raw)

    def test_utf8_holds_empty_multibyte_and_long_values(self):
        values = ["", "é", "日本語", "x" * 500, "trailing"]
        stream = arrow_ipc.SaciStream()
        stream.write_component("S", 1, arrow_ipc.Utf8Column("text", values))
        stream.write_alive([True] * len(values))

        batch = arrow_ipc.SaciStream(stream.to_bytes()).component("S")
        self.assertEqual(batch.strings("text"), values)

    def test_int64_round_trips_the_full_signed_range(self):
        values = [0, 1, -1, 2**63 - 1, -(2**63)]
        stream = arrow_ipc.SaciStream()
        stream.write_component("S", 1, arrow_ipc.Int64Column("n", values))
        stream.write_alive([True] * len(values))

        batch = arrow_ipc.SaciStream(stream.to_bytes()).component("S")
        self.assertEqual(batch.int64s("n"), values)

    def test_float64_round_trips_signed_zero_and_infinities(self):
        values = [0.0, -0.0, math.inf, -math.inf, _ALL_BYTES_SET]
        stream = arrow_ipc.SaciStream()
        stream.write_component("S", 1, arrow_ipc.Float64Column("x", values))
        stream.write_alive([True] * len(values))

        read = arrow_ipc.SaciStream(stream.to_bytes()).component("S").float64s("x")
        self.assertEqual(read, values)
        # -0.0 == 0.0, so the sign bit needs its own assertion.
        self.assertEqual(math.copysign(1.0, read[1]), -1.0)

    def test_bools_pack_across_byte_boundaries(self):
        values = [i % 3 == 0 for i in range(17)]
        stream = arrow_ipc.SaciStream()
        stream.write_component("S", 1, arrow_ipc.BoolColumn("b", values))
        stream.write_alive([True] * len(values))

        batch = arrow_ipc.SaciStream(stream.to_bytes()).component("S")
        self.assertEqual(batch.rows, 17)
        self.assertEqual(batch.bools("b"), values)

    def test_one_row_and_wide_batches_agree_with_their_bitmap(self):
        for rows in (1, 7, 8, 9, 64):
            with self.subTest(rows=rows):
                stream = arrow_ipc.SaciStream()
                stream.write_component(
                    "S", 1, arrow_ipc.BoolColumn("b", [True] * rows)
                )
                stream.write_alive([True] * rows)
                batch = arrow_ipc.SaciStream(stream.to_bytes()).component("S")
                self.assertEqual(batch.bools("b"), [True] * rows)

    def test_a_component_may_be_shorter_than_the_bitmap(self):
        stream = arrow_ipc.SaciStream()
        stream.write_component("S", 1, arrow_ipc.Int64Column("n", [1, 2]))
        stream.write_alive([True] * 5)

        parsed = arrow_ipc.SaciStream(stream.to_bytes())
        self.assertEqual(parsed.component("S").rows, 2)
        self.assertEqual(parsed.component("__alive").rows, 5)


class ReEncodeTest(unittest.TestCase):
    """Rewriting a component, which is what an in-place patch cannot do."""

    def test_a_shorter_batch_replaces_the_longer_one(self):
        stream = arrow_ipc.SaciStream()
        stream.write_component(
            "Order",
            1,
            arrow_ipc.Int64Column("id", [1, 2, 3, 4, 5]),
            arrow_ipc.Utf8Column("tag", ["a", "bb", "ccc", "dddd", "eeeee"]),
        )
        stream.write_alive([True] * 5)
        stream.write_component(
            "Order",
            1,
            arrow_ipc.Int64Column("id", [7, 8, 9]),
            arrow_ipc.Utf8Column("tag", ["x", "yy", "zzz"]),
        )

        batch = arrow_ipc.SaciStream(stream.to_bytes()).component("Order")
        self.assertEqual(batch.rows, 3)
        self.assertEqual(batch.int64s("id"), [7, 8, 9])
        self.assertEqual(batch.strings("tag"), ["x", "yy", "zzz"])

    def test_rewriting_one_component_leaves_the_others_byte_identical(self):
        original = _built().to_bytes()
        stream = arrow_ipc.SaciStream(original)
        stream.write_component(
            "Order",
            1,
            arrow_ipc.Int64Column("id", [1, 2, 3]),
            arrow_ipc.Utf8Column("region", ["EMEA", "APAC", "AMER"]),
            arrow_ipc.Float64Column("amount", [1.5, -2.25, _ALL_BYTES_SET]),
            arrow_ipc.BoolColumn("valid", [True, False, True]),
        )
        rewritten = stream.to_bytes()

        # The untouched segments come out of the parse, not the encoder.
        self.assertEqual(_segments(rewritten)["Ledger"], _segments(original)["Ledger"])
        self.assertEqual(
            _segments(rewritten)["__alive"], _segments(original)["__alive"]
        )
        batch = arrow_ipc.SaciStream(rewritten).component("Order")
        self.assertEqual(batch.strings("region"), ["EMEA", "APAC", "AMER"])

    def test_an_in_place_patch_survives_a_sibling_rewrite(self):
        original = _built().to_bytes()
        stream = arrow_ipc.SaciStream(original)
        stream.component("Ledger").set_int64("seq", 1, 99)
        stream.write_component("Order", 1, arrow_ipc.Int64Column("id", [4, 5, 6]))

        parsed = arrow_ipc.SaciStream(stream.to_bytes())
        self.assertEqual(parsed.component("Ledger").int64s("seq"), [10, 99, 10])
        self.assertEqual(parsed.component("Order").field_names, ["id"])

    def test_a_parsed_stream_with_no_writes_is_returned_verbatim(self):
        original = _built().to_bytes()
        self.assertEqual(arrow_ipc.SaciStream(original).to_bytes(), original)


class WriterRejectionTest(unittest.TestCase):
    """Every refusal is a `ValueError`, the same as a malformed stream."""

    def test_columns_must_agree_on_a_row_count(self):
        stream = arrow_ipc.SaciStream()
        with self.assertRaises(ValueError):
            stream.write_component(
                "S",
                1,
                arrow_ipc.Int64Column("a", [1, 2]),
                arrow_ipc.Int64Column("b", [1]),
            )

    def test_a_segment_needs_a_column(self):
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream().write_component("S", 1)

    def test_duplicate_column_names_are_refused(self):
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream().write_component(
                "S",
                1,
                arrow_ipc.Int64Column("a", [1]),
                arrow_ipc.Float64Column("a", [1.0]),
            )

    def test_a_column_name_must_be_a_non_empty_string(self):
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream().write_component("S", 1, arrow_ipc.Int64Column("", [1]))

    def test_a_plain_list_is_not_a_column(self):
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream().write_component("S", 1, [1, 2, 3])

    def test_the_component_name_and_version_are_validated(self):
        stream = arrow_ipc.SaciStream()
        column = arrow_ipc.Int64Column("a", [1])
        for name, version in (("", 1), (None, 1), ("__alive", 1)):
            with self.assertRaises(ValueError):
                stream.write_component(name, version, column)
        for version in (-1, 2**32, 1.0, "1", True):
            with self.assertRaises(ValueError):
                stream.write_component("S", version, column)

    def test_an_out_of_range_int_is_refused(self):
        stream = arrow_ipc.SaciStream()
        for value in (2**63, -(2**63) - 1, 1.5):
            with self.assertRaises(ValueError):
                stream.write_component("S", 1, arrow_ipc.Int64Column("n", [value]))

    def test_a_non_numeric_float_is_refused(self):
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream().write_component(
                "S", 1, arrow_ipc.Float64Column("x", ["nope"])
            )

    def test_a_non_string_utf8_value_is_refused(self):
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream().write_component(
                "S", 1, arrow_ipc.Utf8Column("text", [b"bytes"])
            )

    def test_a_stream_without_a_bitmap_cannot_be_serialized(self):
        stream = arrow_ipc.SaciStream()
        with self.assertRaises(ValueError):
            stream.to_bytes()
        stream.write_component("S", 1, arrow_ipc.Int64Column("n", [1]))
        with self.assertRaises(ValueError):
            stream.to_bytes()

    def test_a_component_longer_than_the_bitmap_is_refused(self):
        stream = arrow_ipc.SaciStream()
        stream.write_component("S", 1, arrow_ipc.Int64Column("n", [1, 2, 3]))
        stream.write_alive([True, True])
        with self.assertRaises(ValueError):
            stream.to_bytes()

    def test_a_written_component_cannot_be_read_back_unparsed(self):
        stream = arrow_ipc.SaciStream()
        stream.write_component("S", 1, arrow_ipc.Int64Column("n", [1]))
        with self.assertRaises(ValueError):
            stream.component("S")


class SchemaIpcTest(unittest.TestCase):
    """The schema-only stream a processor reports in its descriptor."""

    def test_it_carries_the_columns_in_order(self):
        raw = arrow_ipc.schema_ipc(
            arrow_ipc.Int64Column("id", ()),
            arrow_ipc.Utf8Column("region", ()),
            arrow_ipc.Float64Column("amount", ()),
            arrow_ipc.BoolColumn("valid", ()),
        )
        self.assertEqual(
            _schema_fields(raw),
            [
                ("id", arrow_ipc.TYPE_INT),
                ("region", arrow_ipc.TYPE_UTF8),
                ("amount", arrow_ipc.TYPE_FLOAT),
                ("valid", arrow_ipc.TYPE_BOOL),
            ],
        )

    def test_it_is_not_a_segment(self):
        """No `__saci_component` metadata, so the reader refuses it as a stream.

        A descriptor's bytes and a wire segment's schema are different things,
        and reusing one for the other is the mistake this asserts against.
        """
        raw = arrow_ipc.schema_ipc(arrow_ipc.Int64Column("id", ()))
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream(struct.pack("<I", len(raw)) + raw + b"\x00" * 4)

    def test_a_column_is_still_required(self):
        with self.assertRaises(ValueError):
            arrow_ipc.schema_ipc()


@unittest.skipUnless(os.path.isfile(_ORDER_SCHEMA), _MISSING)
class ArrowRsSchemaTest(unittest.TestCase):
    """The descriptor arrow-rs writes for `Order`, field for field."""

    def test_schema_ipc_matches_the_generated_descriptor(self):
        with open(_ORDER_SCHEMA, "rb") as handle:
            canonical = handle.read()
        columns = [_column(name, type_type) for name, type_type in _ORDER_FIELDS]
        self.assertEqual(
            _schema_fields(arrow_ipc.schema_ipc(*columns)),
            _schema_fields(canonical),
        )


@unittest.skipUnless(os.path.isfile(_FIXTURE_SACI), _MISSING)
class ArrowRsLayoutTest(unittest.TestCase):
    """The body layout arrow-rs writes, re-encoded from its own values.

    arrow-rs pads every buffer to 64 bytes and this writer pads to 8, so the
    offsets differ by design. Everything that describes the data — the row
    count, one all-valid field node per field, and the buffer *lengths* in slot
    order — must agree, or the two encoders disagree about the format.
    """

    def test_buffer_lengths_and_field_nodes_agree(self):
        with open(_FIXTURE_SACI, "rb") as handle:
            canonical = handle.read()
        source = arrow_ipc.SaciStream(canonical)
        batch = source.component("Order")

        stream = arrow_ipc.SaciStream()
        stream.write_component(
            "Order",
            1,
            *[
                _column(name, type_type, batch)
                for name, type_type in zip(batch.field_names, batch._types)
            ],
        )
        stream.write_alive(source.component("__alive").bools("alive"))

        for name in ("Order", "__alive"):
            with self.subTest(component=name):
                want = _batch_shape(_segments(canonical)[name])
                got = _batch_shape(_segments(stream.to_bytes())[name])
                self.assertEqual(got, want)


def _column(name, type_type, batch=None):
    """A column of `type_type`, empty or holding `batch`'s values for `name`."""
    reader, column = {
        arrow_ipc.TYPE_INT: ("int64s", arrow_ipc.Int64Column),
        arrow_ipc.TYPE_FLOAT: ("float64s", arrow_ipc.Float64Column),
        arrow_ipc.TYPE_BOOL: ("bools", arrow_ipc.BoolColumn),
        arrow_ipc.TYPE_UTF8: ("strings", arrow_ipc.Utf8Column),
    }[type_type]
    return column(name, () if batch is None else getattr(batch, reader)(name))


def _segments(stream):
    """`{component name: segment bytes}`, framing excluded."""
    out = {}
    buf = bytearray(stream)
    for start, length in arrow_ipc._split_segments(buf):
        name, _batch = arrow_ipc._parse_segment(buf, start, length)
        out[name] = bytes(buf[start : start + length])
    return out


def _batch_shape(segment):
    """`(rows, field nodes, buffer lengths)` of a segment's RecordBatch."""
    buf = bytearray(segment)
    for header_type, header, _body_start, _body_length in arrow_ipc._iter_messages(
        buf, 0, len(buf)
    ):
        if header_type != arrow_ipc._HEADER_RECORD_BATCH:
            continue
        nodes_at, node_count = header.vector(arrow_ipc._RB_NODES, 16)
        nodes = [
            (
                arrow_ipc._read(arrow_ipc._I64, buf, nodes_at + 16 * i),
                arrow_ipc._read(arrow_ipc._I64, buf, nodes_at + 16 * i + 8),
            )
            for i in range(node_count)
        ]
        buffers_at, buffer_count = header.vector(arrow_ipc._RB_BUFFERS, 16)
        lengths = [
            arrow_ipc._read(arrow_ipc._I64, buf, buffers_at + 16 * i + 8)
            for i in range(buffer_count)
        ]
        return header.scalar(arrow_ipc._RB_LENGTH, arrow_ipc._I64, 0), nodes, lengths
    raise AssertionError("segment carries no record batch")


if __name__ == "__main__":
    unittest.main()
