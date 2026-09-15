"""testdata/records/unclaimed-hop0.jsonl through the generated Python reader [LOG-I16].

Python only reads and writes records; no Python component accepts records as a
receiving service. This proves the reader keeps the service's unclaimed hop 0.
"""
from pathlib import Path
import unittest

from abstraction.logging import rec

DATA = Path(__file__).resolve().parents[1] / "testdata" / "records" / "unclaimed-hop0.jsonl"


class UnclaimedHopZeroTests(unittest.TestCase):
    def check(self, record):
        self.assertEqual(len(record.identity), 2)
        first, stamp = record.identity
        self.assertEqual((first.by, first.verified, first.hop, first.uid, first.gid, first.pid, first.program, first.exe),
                         ("unclaimed", False, 0, -1, -1, -1, "", ""))
        self.assertEqual((stamp.by, stamp.verified, stamp.hop, stamp.exe),
                         ("identity/windows", True, 1, "C:\\Users\\oa\\Python312\\python.exe"))
        self.assertEqual(record.attrs, {"logging.writer_claim": "absent"})

    def test_reader_keeps_the_unclaimed_hop_zero(self):
        record = rec.decode(DATA.read_bytes())
        self.check(record)
        self.check(rec.decode(rec.encode(record)))


if __name__ == "__main__":
    unittest.main()
