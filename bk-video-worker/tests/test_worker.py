import os
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import worker


class WorkerUtilityTests(unittest.TestCase):
    def test_env_values_are_trimmed(self):
        with patch.dict(os.environ, {"QUALITY_TEST_VALUE": "  ready\n"}):
            self.assertEqual(worker._envstr("QUALITY_TEST_VALUE"), "ready")

    def test_extracts_supported_youtube_urls(self):
        self.assertEqual(
            worker._extract_youtube_id("https://youtu.be/abc123?t=10"),
            "abc123",
        )
        self.assertEqual(
            worker._extract_youtube_id("https://www.youtube.com/watch?v=xyz789&feature=share"),
            "xyz789",
        )
        self.assertIsNone(worker._extract_youtube_id("https://example.com/video"))

    def test_now_iso_is_timezone_aware(self):
        timestamp = datetime.fromisoformat(worker.now_iso())

        self.assertIsNotNone(timestamp.tzinfo)

    def test_bridge_generation_is_skipped_without_a_key(self):
        with patch.object(worker, "GEMINI_API_KEY", ""):
            self.assertEqual(worker.generate_bridge_doc("Title", "Post content"), "")


if __name__ == "__main__":
    unittest.main()
