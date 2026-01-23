import logging
import pytest
from unittest.mock import MagicMock, call

from src.utils import is_ewf_files, process_plaso_cli_logs


class TestIsEwfFiles:
    """Test cases for the is_ewf_files function."""

    def test_all_valid_ewf_files(self):
        """Test that function returns True when all files have valid EWF extensions."""
        input_files = [
            {"path": "/path/to/image.e01"},
            {"path": "/path/to/image.e02"},
            {"path": "/path/to/image.e03"},
        ]
        assert is_ewf_files(input_files) is True

    def test_single_valid_ewf_file(self):
        """Test that function returns True for a single valid EWF file."""
        input_files = [{"path": "/path/to/evidence.e01"}]
        assert is_ewf_files(input_files) is True

    def test_case_insensitive_extensions(self):
        """Test that function handles case-insensitive extensions correctly."""
        input_files = [
            {"path": "/path/to/image.E01"},
            {"path": "/path/to/image.E02"},
            {"path": "/path/to/image.e03"},
        ]
        assert is_ewf_files(input_files) is True

    def test_mixed_file_types(self):
        """Test that function returns False when files have mixed extensions."""
        input_files = [
            {"path": "/path/to/image.e01"},
            {"path": "/path/to/document.txt"},
            {"path": "/path/to/image.e02"},
        ]
        assert is_ewf_files(input_files) is False

    def test_no_ewf_files(self):
        """Test that function returns False when no files have EWF extensions."""
        input_files = [
            {"path": "/path/to/document.txt"},
            {"path": "/path/to/image.jpg"},
            {"path": "/path/to/data.bin"},
        ]
        assert is_ewf_files(input_files) is False

    def test_empty_list(self):
        """Test that function returns True for empty input list."""
        input_files = []
        assert is_ewf_files(input_files) is True

    def test_missing_path_key(self):
        """Test that function handles missing 'path' key gracefully."""
        input_files = [
            {"path": "/path/to/image.e01"},
            {"name": "missing_path_key"},  # No 'path' key
            {"path": "/path/to/image.e02"},
        ]
        assert is_ewf_files(input_files) is False

    def test_empty_path_value(self):
        """Test that function handles empty path values."""
        input_files = [
            {"path": "/path/to/image.e01"},
            {"path": ""},  # Empty path
            {"path": "/path/to/image.e02"},
        ]
        assert is_ewf_files(input_files) is False

    def test_boundary_extensions(self):
        """Test that function works with boundary EWF extensions (.e01 and .e99)."""
        input_files = [
            {"path": "/path/to/image.e01"},
            {"path": "/path/to/image.e99"},
        ]
        assert is_ewf_files(input_files) is True

    def test_invalid_ewf_extension(self):
        """Test that function returns False for invalid EWF-like extensions."""
        input_files = [
            {"path": "/path/to/image.e01"},
            {"path": "/path/to/image.e100"},  # Out of range
            {"path": "/path/to/image.e00"},  # Out of range
        ]
        assert is_ewf_files(input_files) is False

    def test_ewf_extension_with_additional_extension(self):
        """Test that function returns False for files with additional extensions."""
        input_files = [
            {"path": "/path/to/image.e01"},
            {"path": "/path/to/image.e01.bak"},  # Additional extension
        ]
        assert is_ewf_files(input_files) is False

    @pytest.mark.parametrize("extension", [".e01", ".e15", ".e50", ".e99"])
    def test_various_valid_extensions(self, extension):
        """Test that function returns True for various valid EWF extensions."""
        input_files = [{"path": f"/path/to/image{extension}"}]
        assert is_ewf_files(input_files) is True

    @pytest.mark.parametrize("extension", [".e00", ".e100", ".e999", ".exe01"])
    def test_various_invalid_extensions(self, extension):
        """Test that function returns False for various invalid extensions."""
        input_files = [{"path": f"/path/to/image{extension}"}]
        assert is_ewf_files(input_files) is False


class TestProcessPlasoCliLogs:
    def test_process_plaso_cli_logs_basic(self):
        """Test standard single-line log parsing."""
        mock_logger = MagicMock()
        logs = "[INFO] Starting process\n[WARNING] Low disk space"

        process_plaso_cli_logs(logs, mock_logger)

        expected_calls = [
            call(logging.INFO, "Starting process"),
            call(logging.WARNING, "Low disk space"),
        ]
        mock_logger.log.assert_has_calls(expected_calls)

    def test_process_plaso_cli_logs_multiline_continuation(self):
        """Test that lines without a header use the previous level."""
        mock_logger = MagicMock()
        logs = "[ERROR] Database failure\n  at line 50\n  at line 52"

        process_plaso_cli_logs(logs, mock_logger)

        expected_calls = [
            call(logging.ERROR, "Database failure"),
            call(logging.ERROR, "  at line 50"),
            call(logging.ERROR, "  at line 52"),
        ]
        mock_logger.log.assert_has_calls(expected_calls)

    def test_process_plaso_cli_logs_unknown_level(self):
        """Test fallback to default level when header is unrecognized."""
        mock_logger = MagicMock()
        logs = "[UNKNOWNEVEL] This should be info"

        process_plaso_cli_logs(logs, mock_logger)

        mock_logger.log.assert_called_once_with(logging.INFO, "This should be info")

    def test_process_plaso_cli_logs_empty_input(self):
        """Test that empty lines are skipped."""
        mock_logger = MagicMock()
        logs = "\n\n  \n"

        process_plaso_cli_logs(logs, mock_logger)

        mock_logger.log.assert_not_called()
