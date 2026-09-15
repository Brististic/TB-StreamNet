import unittest
from unittest.mock import patch

from processing.spark_streaming import _neo4j_config


class ProcessingConfigurationTests(unittest.TestCase):
    @patch.dict(
        "os.environ",
        {
            "NEO4J_URI": "bolt://example:7687",
            "NEO4J_USER": "operator",
            "NEO4J_PASSWORD": "secret",
            "NEO4J_DATABASE": "analytics",
        },
        clear=False,
    )
    def test_neo4j_configuration_uses_environment(self):
        self.assertEqual(
            _neo4j_config(),
            ("bolt://example:7687", "operator", "secret", "analytics"),
        )


if __name__ == "__main__":
    unittest.main()
