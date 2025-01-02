import pytest
import os
from ingestion.src.main import load_delta_table_mapping


class TestLoadDeltaTableMapping:
    @pytest.fixture
    def dummy_mapping_file(self, tmp_path):
        """Create a dummy mapping file for testing."""
        mapping_content = """
        event_table_mapping:
          user-events:
            created: "./delta_lake/user_created"
            deleted: "./delta_lake/user_deleted"
        """
        mapping_file = tmp_path / "delta_table_mapping.yaml"
        with open(mapping_file, "w") as f:
            f.write(mapping_content)
        return mapping_file

    def test_load_mapping_success(self, dummy_mapping_file):
        """Test loading of Delta table mapping from YAML file."""
        result = load_delta_table_mapping(str(dummy_mapping_file))
        expected = {
            "user-events": {
                "created": "./delta_lake/user_created",
                "deleted": "./delta_lake/user_deleted",
            }
        }
        assert (
            result == expected
        ), "The Delta table mapping does not match the expected output."

    def test_load_mapping_file_not_found(self):
        """Test behavior when the mapping file does not exist."""
        result = load_delta_table_mapping("non_existent_file.yaml")
        assert (
            result == {}
        ), "Expected an empty dictionary when the file does not exist."
