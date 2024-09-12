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
