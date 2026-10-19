import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from abstraction import logging as wire


class ReplyTransport:
    def __init__(self, value):
        self.value = value

    def exchange_frame(self, raw):
        request = json.loads(raw)
        return json.dumps(dict(version=1, service=request["service"], method=request["method"],
                               ok=True, payload={"value": self.value})).encode()


class HistoryOutcomeTests(unittest.TestCase):
    def test_new_clients_return_typed_business_results(self):
        for outcome in ("not_permitted", "forbidden", "invalid", "unavailable",
                        "os_authorization_required", "os_authorization_pending"):
            with self.subTest(outcome=outcome):
                result = dict(outcome=outcome, records=[], next="supplied", at_end=False)
                transport = ReplyTransport(result)
                source = wire.SourceReaderClient(transport).read("windows/application", "supplied", 1, 65536)
                self.assertEqual((source.outcome, source.next, source.records, source.at_end),
                                 (outcome, "supplied", [], False))
                target = wire.ApplicationLogTarget(selection=wire.ApplicationLogSelection.ALL_APPS)
                self.assertEqual(wire.ApplicationHistoryReaderClient(transport).read(target, "supplied", 1, 65536).outcome, outcome)
                self.assertEqual(wire.ApplicationHistoryObserverClient(transport).observe(target, "supplied", 1, 65536, 0).outcome, outcome)
        with self.assertRaises(wire.Refusal):
            wire.SourceReaderClient(ReplyTransport(dict(outcome="future_action", records=[], next="", at_end=False))).read("windows/application", "", 1, 65536)

    def test_catalogue_preserves_descriptions_and_refuses_unknown_behavior(self):
        value = dict(outcome="catalog", sources=[dict(id="windows/application", availability="available", limitations=["future_constraint"])])
        catalog = wire.SourceReaderClient(ReplyTransport(value)).sources()
        self.assertEqual(catalog.outcome, "catalog")
        self.assertEqual(catalog.sources[0].limitations, ["future_constraint"])
        self.assertIn(b'"future_constraint"', wire.encode_source_catalog(catalog))
        for bad in (dict(sources=[]), dict(outcome="future_action", sources=[]),
                    dict(outcome="catalog", sources=[dict(id="windows/application", availability="future_state", limitations=[])])):
            with self.subTest(bad=bad), self.assertRaises(wire.Refusal):
                wire.decode_source_catalog(json.dumps(bad).encode())

    def test_legacy_page_retains_published_word_and_shape(self):
        page = wire.Page(outcome=wire.PageOutcome.INVALID_REQUEST, records=[], next="supplied", at_end=False)
        self.assertEqual(wire.encode_page(page), b'{\n  "outcome": "invalid_request",\n  "records": [],\n  "next": "supplied",\n  "at_end": false\n}\n')
        with self.assertRaises(wire.Refusal):
            wire.decode_page(b'{"outcome":"invalid","records":[],"next":"supplied","at_end":false}')


if __name__ == "__main__":
    unittest.main()
