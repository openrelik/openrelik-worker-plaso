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
        input_file.get("path", "").lower().endswith(ewf_extensions) for input_file in input_files
    )
    return is_ewf_files
