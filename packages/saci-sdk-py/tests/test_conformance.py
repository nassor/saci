"""Runs the shared conformance corpus at `packages/arrow-ipc-conformance`.

One corpus, five codecs: this module is the Python end of it, so a stream this
codec accepts and the Go, TypeScript, Kotlin or C# codec refuses shows up here
instead of in production traffic.

The error text stays local to each language, so the only thing shared is the
reason code. `_SUBSTRINGS` is the whole translation layer: a new corpus case
needs one row there and nothing else.

A missing manifest or a missing vector fails. It never skips: a conformance
suite that quietly runs zero cases is worse than no suite at all, because it
reports green.
"""

import json
import pathlib
import unittest

from saci_sdk import arrow_ipc

#: The corpus is a sibling package, one copy for every codec.
_CORPUS = pathlib.Path(__file__).resolve().parents[2] / "arrow-ipc-conformance"
_MANIFEST = _CORPUS / "manifest.json"

_REGENERATE = (
    "cargo run -p saci-service --features conformance --example conformance_vectors -- emit"
)

#: Reason code -> a substring of the message this codec raises for that rule.
#: Every entry is checked for ambiguity by `test_every_case`, so a rule whose
#: substring also matches a neighbouring rule's message is a test failure rather
#: than a silent pass on the wrong error.
_SUBSTRINGS = {
    "trailing_bytes": "follow the stream terminator",
    "truncated_stream": "stream is truncated",
    "truncated_message": "but the segment ends at",
    "bad_continuation": "lacks the 0xffffffff continuation marker",
    "empty_segment": "is empty: it carries no schema message",
    "first_message_not_schema": "record batch precedes its schema message",
    "second_message_not_record_batch": "is not supported (expected",
    "dictionary_batch": "dictionary batches are not supported",
    "compressed_batch": "compressed record batches are not supported",
    "extra_message": "carries an extra",
    "bad_row_count": "which is not a usable length",
    "nodes_field_mismatch": "field nodes but the schema declares",
    "buffer_overruns_body": "overrunning the",
    "missing_component_key": "__saci_component",
    "unknown_component": "no segment for component",
    "unknown_field": "no field named",
    "type_mismatch": "not the requested",
    "row_out_of_range": "is outside the batch's",
    "variable_width_write": "variable-length column",
}

#: Column type -> reader. `float64` values are compared exactly: the corpus
#: carries round-tripped bit patterns, not computed results, so a tolerance
#: would hide a byte-order or offset mistake.
_READERS = {
    "int64": "int64s",
    "float64": "float64s",
    "bool": "bools",
    "utf8": "strings",
}

#: Column type -> in-place setter. There is no `set_utf8`: a fixed-width setter
#: is the only write this codec can attempt on a `Utf8` column, and refusing it
#: is exactly the `variable_width_write` rule, so the value never reaches the
#: pack step.
_WRITERS = {
    "int64": "set_int64",
    "float64": "set_float64",
    "bool": "set_bool",
    "utf8": "set_float64",
}

_OP_KINDS = ("component", "column", "set")


class ConformanceTest(unittest.TestCase):
    """Every corpus case, each as its own subtest named after the case."""

    @classmethod
    def setUpClass(cls):
        if not _MANIFEST.is_file():
            raise AssertionError(
                "conformance manifest missing at {}; regenerate with `{}`".format(
                    _MANIFEST, _REGENERATE
                )
            )
        cls.manifest = json.loads(_MANIFEST.read_text(encoding="utf-8"))

    def test_reason_codes_are_all_mapped(self):
        """The table covers the corpus exactly, in both directions."""
        self.assertEqual(sorted(self.manifest["reasons"]), sorted(_SUBSTRINGS))

    def test_every_case(self):
        cases = self.manifest["cases"]
        self.assertTrue(cases, "manifest at {} carries no cases".format(_MANIFEST))
        for case in cases:
            with self.subTest(name=case["name"]):
                self._run_case(case)

    # -- one case ----------------------------------------------------------

    def _run_case(self, case):
        vector = _CORPUS / case["vector"]
        if not vector.is_file():
            self.fail(
                "case {!r}: vector {} is missing; regenerate with `{}`".format(
                    case["name"], vector, _REGENERATE
                )
            )
        raw = vector.read_bytes()
        op = case.get("op")
        if op is not None and op["kind"] not in _OP_KINDS:
            self.fail("case {!r}: unknown op kind {!r}".format(case["name"], op["kind"]))
        if case["expect"] == "accept":
            self._expect_accept(case, raw)
        else:
            self._expect_reject(case, raw)

    def _expect_accept(self, case, raw):
        want = case["accept"]
        stream = self._guard(case, lambda: arrow_ipc.SaciStream(raw))
        self.assertEqual(stream.component_names, want["components"])
        batch = self._guard(case, lambda: stream.component(want["component"]))
        self.assertEqual(batch.rows, want["rows"])
        for field, column in want["columns"].items():
            values = self._guard(case, lambda: _read(batch, field, column["type"]))
            self.assertEqual(values, column["values"], "column {!r}".format(field))

    def _expect_reject(self, case, raw):
        reason = case["reason"]
        substring = _SUBSTRINGS.get(reason)
        if substring is None:
            self.fail("case {!r}: reason {!r} has no mapped substring".format(case["name"], reason))
        try:
            _perform(case, raw)
        except ValueError as error:
            # `ValueError` is this codec's rejection type, and everything it
            # raises through goes out as one: a caller cannot tell a builtin
            # `struct.error` or `IndexError` from a bug in the codec.
            message = str(error)
        except Exception as error:  # a leaked builtin is the failure, so catch it
            self.fail(
                "case {!r}: raised {} instead of the codec's ValueError: {}".format(
                    case["name"], type(error).__name__, error
                )
            )
        else:
            self.fail(
                "case {!r}: accepted a stream the corpus refuses for {!r}".format(
                    case["name"], reason
                )
            )
        self.assertIn(substring, message, "case {!r} expects reason {!r}".format(case["name"], reason))
        overlap = sorted(
            other for other, text in _SUBSTRINGS.items() if other != reason and text in message
        )
        self.assertEqual(
            overlap,
            [],
            "case {!r}: message for {!r} also matches {}".format(case["name"], reason, overlap),
        )

    def _guard(self, case, call):
        """Run `call`, reporting any raise as a failure naming the case."""
        try:
            return call()
        except Exception as error:  # a leaked builtin is the failure, so catch it
            self.fail(
                "case {!r}: {} from a stream the corpus accepts: {}".format(
                    case["name"], type(error).__name__, error
                )
            )


def _read(batch, field, type_name):
    return getattr(batch, _READERS[type_name])(field)


def _perform(case, raw):
    """Parse the vector and run the case's `op`, if it carries one."""
    stream = arrow_ipc.SaciStream(raw)
    op = case.get("op")
    if op is None:
        return
    if op["kind"] == "component":
        stream.component(op["component"])
        return
    batch = stream.component(op["component"])
    if op["kind"] == "column":
        _read(batch, op["field"], op["type"])
        return
    getattr(batch, _WRITERS[op["type"]])(op["field"], op["row"], op["value"])


if __name__ == "__main__":
    unittest.main()
