"""Native tests for the Arrow IPC codec.

Run with `PYTHONPATH=src python -m unittest discover -s tests` from the package
root. No wasm involved: this is where flatbuffer-offset mistakes surface,
seconds after they are made, instead of as an opaque trap inside a componentized
processor.

Ground truth is the emitter's own output: `fixture_input.saci` decoded here must
equal `fixture_input.json` written by arrow-rs.
"""

import json
import os
import struct
import unittest

from saci_sdk import arrow_ipc

_HERE = os.path.dirname(os.path.abspath(__file__))
_GENERATED = os.path.normpath(
    os.path.join(_HERE, "..", "..", "..", "examples", "polyglot", "generated")
)
_FIXTURE_SACI = os.path.join(_GENERATED, "fixture_input.saci")
_FIXTURE_JSON = os.path.join(_GENERATED, "fixture_input.json")

_HAVE_FIXTURES = os.path.isfile(_FIXTURE_SACI) and os.path.isfile(_FIXTURE_JSON)
_MISSING = (
    "generated fixtures absent; run `cargo run -p saci-service --features wasm "
    "--example polyglot_schema_emit -- emit`"
)

_INT_FIELDS = ("id", "review_tier")
_STRING_FIELDS = ("region", "currency", "usd_amount_display", "settlement")
_FLOAT_FIELDS = ("amount", "usd_amount", "risk_score", "fee")
_BOOL_FIELDS = ("valid", "flagged")

#: Every byte of this value's IEEE-754 encoding is non-zero (0x40a1111111111111),
#: so overwriting a zeroed slot with it must flip exactly eight bytes. A round
#: number like 6800.0 would only flip three and hide a short write.
_ALL_BYTES_SET = float.fromhex("0x1.1111111111111p+11")

#: Every byte of this integer's little-endian encoding is non-zero, so it flips
#: exactly eight bytes in a zeroed slot, the way `_ALL_BYTES_SET` does for
#: floats. A small tier like 2 would only flip one and hide a short write.
_ALL_BYTES_SET_INT = 0x0102030405060708


@unittest.skipUnless(_HAVE_FIXTURES, _MISSING)
class ArrowIpcTest(unittest.TestCase):
    """Decode, mutate, and re-decode the canonical six-row fixture."""

    @classmethod
    def setUpClass(cls):
        with open(_FIXTURE_SACI, "rb") as handle:
            cls.raw = handle.read()
        with open(_FIXTURE_JSON, "r", encoding="utf-8") as handle:
            cls.expected = json.load(handle)

    def order(self, raw=None):
        stream = arrow_ipc.SaciStream(self.raw if raw is None else raw)
        return stream, stream.component("Order")

    def columns(self, batch):
        """Every column of the batch as a list of Python values."""
        out = {}
        for name in _INT_FIELDS:
            out[name] = batch.int64s(name)
        for name in _FLOAT_FIELDS:
            out[name] = batch.float64s(name)
        for name in _BOOL_FIELDS:
            out[name] = batch.bools(name)
        for name in _STRING_FIELDS:
            out[name] = batch.strings(name)
        return out

    # -- decoding ----------------------------------------------------------

    def test_segments_are_order_then_alive(self):
        stream = arrow_ipc.SaciStream(self.raw)
        self.assertEqual(stream.component_names, ["Order", "__alive"])
        self.assertEqual(stream.component("__alive").rows, len(self.expected))

    def test_schema_field_order(self):
        _stream, batch = self.order()
        self.assertEqual(
            batch.field_names,
            [
                "id",
                "region",
                "currency",
                "amount",
                "valid",
                "usd_amount",
                "usd_amount_display",
                "risk_score",
                "flagged",
                "fee",
                "review_tier",
                "settlement",
            ],
        )

    def test_every_column_matches_the_json_fixture(self):
        _stream, batch = self.order()
        self.assertEqual(batch.rows, len(self.expected))
        actual = self.columns(batch)
        for row, want in enumerate(self.expected):
            for name in _INT_FIELDS + _BOOL_FIELDS + _STRING_FIELDS:
                self.assertEqual(actual[name][row], want[name], "{}[{}]".format(name, row))
            for name in _FLOAT_FIELDS:
                self.assertAlmostEqual(
                    actual[name][row], want[name], delta=1e-6, msg="{}[{}]".format(name, row)
                )

    def test_stream_round_trips_byte_for_byte(self):
        stream, _batch = self.order()
        self.assertEqual(stream.to_bytes(), self.raw)

    # -- in-place mutation -------------------------------------------------

    def test_set_float64_writes_only_its_own_eight_bytes(self):
        stream, batch = self.order()
        before = self.columns(batch)
        batch.set_float64("usd_amount", 2, _ALL_BYTES_SET)
        mutated = stream.to_bytes()

        changed = [i for i, (a, b) in enumerate(zip(self.raw, mutated)) if a != b]
        self.assertEqual(len(changed), 8, "one f64 must touch exactly 8 bytes")
        self.assertEqual(changed, list(range(changed[0], changed[0] + 8)))

        _stream, reread = self.order(mutated)
        after = self.columns(reread)
        self.assertEqual(after["usd_amount"][2], _ALL_BYTES_SET)
        self.assertEqual(after["usd_amount"][:2], [0.0] * 2)
        self.assertEqual(after["usd_amount"][3:], [0.0] * (batch.rows - 3))
        for name in set(before) - {"usd_amount"}:
            self.assertEqual(after[name], before[name], name)

    def test_set_bool_writes_only_its_own_bit(self):
        stream, batch = self.order()
        before = self.columns(batch)
        for row in (0, 2, 3):
            batch.set_bool("valid", row, True)
        mutated = stream.to_bytes()

        changed = [i for i, (a, b) in enumerate(zip(self.raw, mutated)) if a != b]
        self.assertEqual(len(changed), 1, "six bit-packed rows live in one byte")

        _stream, reread = self.order(mutated)
        after = self.columns(reread)
        self.assertEqual(after["valid"], [True, False, True, True, False, False])
        for name in set(before) - {"valid"}:
            self.assertEqual(after[name], before[name], name)

    def test_set_int64_writes_only_its_own_eight_bytes(self):
        stream, batch = self.order()
        before = self.columns(batch)
        batch.set_int64("review_tier", 4, _ALL_BYTES_SET_INT)
        mutated = stream.to_bytes()

        changed = [i for i, (a, b) in enumerate(zip(self.raw, mutated)) if a != b]
        self.assertEqual(len(changed), 8, "one i64 must touch exactly 8 bytes")
        self.assertEqual(changed, list(range(changed[0], changed[0] + 8)))

        _stream, reread = self.order(mutated)
        after = self.columns(reread)
        self.assertEqual(after["review_tier"][4], _ALL_BYTES_SET_INT)
        self.assertEqual(after["review_tier"][:4], [0] * 4)
        self.assertEqual(after["review_tier"][5:], [0] * (batch.rows - 5))
        for name in set(before) - {"review_tier"}:
            self.assertEqual(after[name], before[name], name)

    def test_set_int64_round_trips_the_full_signed_range(self):
        stream, batch = self.order()
        wanted = [0, 1, 2, -1, -(2**63), 2**63 - 1]
        self.assertEqual(len(wanted), batch.rows)
        for row, value in enumerate(wanted):
            batch.set_int64("review_tier", row, value)

        _stream, reread = self.order(stream.to_bytes())
        self.assertEqual(reread.int64s("review_tier"), wanted)

    def test_set_bool_clears_again(self):
        stream, batch = self.order()
        batch.set_bool("flagged", 3, True)
        batch.set_bool("flagged", 3, False)
        self.assertEqual(stream.to_bytes(), self.raw)

    # -- rejection contract ------------------------------------------------

    def test_unknown_component_raises(self):
        stream = arrow_ipc.SaciStream(self.raw)
        with self.assertRaises(ValueError):
            stream.component("Invoice")

    def test_set_float64_on_utf8_field_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.set_float64("settlement", 0, 1.0)

    def test_set_float64_on_bool_field_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.set_float64("valid", 0, 1.0)

    def test_set_int64_on_utf8_field_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.set_int64("settlement", 0, 1)

    def test_set_int64_on_float_field_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.set_int64("usd_amount", 0, 1)

    def test_unknown_field_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.float64s("no_such_field")

    def test_row_out_of_range_raises(self):
        _stream, batch = self.order()
        with self.assertRaises(ValueError):
            batch.set_float64("usd_amount", batch.rows, 1.0)
        with self.assertRaises(ValueError):
            batch.set_int64("review_tier", batch.rows, 1)
        with self.assertRaises(ValueError):
            batch.set_int64("review_tier", -1, 1)

    def test_truncated_stream_raises_instead_of_crashing(self):
        for cut in (3, 8, 64, 700, len(self.raw) - 1):
            with self.assertRaises(ValueError):
                arrow_ipc.SaciStream(self.raw[:cut])

    def test_corrupt_continuation_marker_raises(self):
        broken = bytearray(self.raw)
        broken[4] = 0x00  # first message's 0xffffffff prefix
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream(bytes(broken))

    def test_bytes_after_a_segments_end_of_stream_marker_raise(self):
        """No corpus vector covers this: `extra_message` uses a second batch.

        Growing the first segment's declared length and filling the gap leaves
        every later segment self-consistent, so only the padding inside the
        first segment is wrong. A reader that stops at the end-of-stream marker
        would silently drop those bytes.
        """
        declared = struct.unpack_from("<I", self.raw, 0)[0]
        padded = bytearray(self.raw)
        padded[4 + declared : 4 + declared] = b"\x00" * 8
        struct.pack_into("<I", padded, 0, declared + 8)
        with self.assertRaises(ValueError):
            arrow_ipc.SaciStream(bytes(padded))

    def test_out_of_range_int64_write_raises_valueerror(self):
        """`struct.error` is not a `ValueError`, so a caller cannot tell it from
        a bug in the codec."""
        _stream, batch = self.order()
        for value in (2**63, -(2**63) - 1):
            with self.assertRaises(ValueError):
                batch.set_int64("review_tier", 0, value)


class DecodeBase64Test(unittest.TestCase):
    """The one encoding helper a processor needs for its schema constant."""

    def test_decodes_a_literal_vector(self):
        self.assertEqual(arrow_ipc.decode_base64("aGVsbG8="), b"hello")

    def test_rejects_a_non_base64_input(self):
        with self.assertRaises(ValueError):
            arrow_ipc.decode_base64("not base64!")


if __name__ == "__main__":
    unittest.main()
