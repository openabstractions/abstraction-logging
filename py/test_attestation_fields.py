"""testdata/records/attestation-fields.jsonl through the generated Python reader."""
from pathlib import Path
import unittest

from abstraction.logging import rec

DATA = Path(__file__).resolve().parents[1] / "testdata" / "records" / "attestation-fields.jsonl"
WANT = [
    ("self", False, 0, "ComfyUI", "", "", -1),
    ("identity/windows", True, 1, "", "S-1-5-21-1-2-3-1001", "C:\\Users\\oa\\Python312\\python.exe", -1),
    ("so_peercred", True, 2, "", "oa", "/usr/bin/python3.14", 1000),
]


class AttestationFieldsTests(unittest.TestCase):
    def hops(self, record):
        return [(a.by, a.verified, a.hop, a.program, a.user, a.exe, a.uid) for a in record.identity]

    def test_program_is_the_claim_and_exe_the_attested_executable(self):
        record = rec.decode(DATA.read_bytes())
        self.assertEqual(self.hops(record), WANT)
        again = rec.decode(rec.encode(record))
        self.assertEqual(self.hops(again), WANT)


if __name__ == "__main__":
    unittest.main()
