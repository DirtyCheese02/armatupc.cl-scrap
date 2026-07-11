from __future__ import annotations

import ast
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PCPP_SCRAPER = REPO_ROOT / "SpecDB" / "Scrap_PCPP.py"


class PcPartPickerCapturePolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = PCPP_SCRAPER.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_capture_is_disabled_by_default_and_requires_permission_reference(self) -> None:
        self.assertIn('parse_bool_env("PCPP_CAPTURE_ENABLED", False)', self.source)
        self.assertIn('os.environ.get("PCPP_PERMISSION_REFERENCE", "").strip()', self.source)
        self.assertIn("if not PCPP_CAPTURE_ENABLED:", self.source)
        self.assertIn("if not PCPP_PERMISSION_REFERENCE:", self.source)

    def test_automatic_turnstile_handler_is_absent(self) -> None:
        function_names = {
            node.name for node in ast.walk(self.tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertNotIn("click_cloudflare_turnstile", function_names)
        self.assertNotIn("checkbox.click", self.source)
        self.assertIn("No automated challenge handling is allowed", self.source)


if __name__ == "__main__":
    unittest.main()
