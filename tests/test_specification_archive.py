import gzip
import json
import tempfile
import unittest
from pathlib import Path

from tools.archive_specifications import archive_specifications


class SpecificationArchiveTests(unittest.TestCase):
    def test_archive_is_verified_before_sources_are_removed(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=repo) as temporary:
            root = Path(temporary)
            source = root / "source"
            (source / "CPU").mkdir(parents=True)
            (source / "GPU").mkdir(parents=True)
            (source / "CPU" / "one.json").write_text('{"Id":"1","Name":"CPU"}', encoding="utf-8")
            (source / "GPU" / "two.json").write_text('{"Id":"2","Name":"GPU"}', encoding="utf-8")
            output = root / "archive.ndjson.gz"
            manifest_path = root / "manifest.json"

            manifest = archive_specifications(source, output, manifest_path, remove_source=True)

            self.assertEqual(manifest["recordCount"], 2)
            self.assertEqual(manifest["categories"], {"CPU": 1, "GPU": 1})
            self.assertFalse(source.exists())
            self.assertTrue(output.is_file())
            with gzip.open(output, "rt", encoding="utf-8") as archived:
                rows = [json.loads(line) for line in archived]
            self.assertEqual([row["sourcePath"] for row in rows], ["CPU/one.json", "GPU/two.json"])
            self.assertTrue(all(len(row["payloadSha256"]) == 64 for row in rows))


if __name__ == "__main__":
    unittest.main()
