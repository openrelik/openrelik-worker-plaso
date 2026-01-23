# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import re


def log2timeline_status_to_dict(status_string: str) -> dict:
    """Convert a log2timeline status string to a dictionary.

    Args:
        status_string: The status string to convert.

    Returns:
        A dictionary containing the status information.
    """
    result_dict = {"tasks": {}}
    items = status_string.split()[1:]

    for name, value in zip(items[::2], items[1::2]):
        result_dict["tasks"][name.strip(":").lower()] = int(value)

    return result_dict


def is_ewf_files(input_files: list[dict]) -> bool:
    """
    Checks if all input files have an EnCase Disk Image (EWF) file extension
    (e.g., .e01, .e02, ..., .e99).

    Args:
        input_files: A list of dictionaries, where each dictionary represents
                     an input file and is expected to have a 'path' key.

    Returns:
        True if all files end with a valid EWF extension, False otherwise.
    """
    # Generate a tuple of valid EWF extensions from .e01 to .e99
    ewf_extensions = tuple(f".e{i:02d}" for i in range(1, 100))

    # Check if all input files end with one of the valid EWF extensions.
    is_ewf_files = all(
        input_file.get("path", "").lower().endswith(ewf_extensions)
        for input_file in input_files
    )
    return is_ewf_files


def process_plaso_cli_logs(cli_logs: str, logger) -> None:
    """Process Plaso CLI log output to relevant Python log sink and level.

    Args:
        cli_logs: The input logs from the plaso CLI tool.
        logger: The logger to use for logging.

    Returns:
        None
    """
    # Matches "[LEVEL] Message" as defined in the log2timeline source code as
    # "format='[%(levelname)s] %(message)s')""
    log_pattern = re.compile(r"^\[(?P<level>\w+)\]\s*(?P<msg>.*)$")

    # Default state
    current_level = logging.INFO

    for line in cli_logs.splitlines():
        line = line.rstrip()
        if not line:
            continue

        match = log_pattern.match(line)

        if match:
            # We found a new header! Update the level.
            # We need this to handle multi-line logs like tracebacks.
            level_str = match.group("level").upper()
            message = match.group("msg")

            # Use INFO as a fallback if getLevelName returns a string (meaning not found)
            new_level = logging.getLevelName(level_str)
            if isinstance(new_level, int):
                current_level = new_level

            logger.log(current_level, f"{message}")
        else:
            # This line doesn't have a [LEVEL] prefix.
            logger.log(current_level, f"{line}")
