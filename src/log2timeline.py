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
import os
import shutil
import subprocess
import time
from uuid import uuid4

from openrelik_worker_common.utils import (
    create_output_file,
    get_input_files,
    task_result,
)
from plaso import __version__ as plaso_version
from plaso.cli import pinfo_tool
from plaso.parsers import manager

from .app import celery
from .utils import log2timeline_status_to_dict

# Get all Plaso parser names to use for user config form.
parser_manager = manager.ParsersManager()
parser_names = {parser for parser, _ in parser_manager.GetParsersInformation()}
for plugin in parser_manager.GetNamesOfParsersWithPlugins():
    for parser, _ in parser_manager.GetParserPluginsInformation(
        parser_filter_expression=plugin
    ):
        parser_names.add(f"{plugin}/{parser}")

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-plaso.tasks.log2timeline"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Log2Timeline",
    "description": "Super timelining",
    "task_config": [
        {
            "name": "artifacts",
            "label": "Select artifacts to parse",
            "description": "Select one or more forensic artifact definitions from the ForensicArtifacts project. These definitions specify files and data relevant to digital forensic investigations.  Only the selected artifacts will be parsed.",
            "type": "artifacts",
            "required": False,
        },
        {
            "name": "parsers",
            "label": "Select parsers to use",
            "description": "Select one or more Plaso parsers. These parsers specify how to interpret files and data. Only data identified by the selected parsers will be processed.",
            "type": "autocomplete",
            "items": parser_names,
            "required": False,
        },
        {
            "name": "process_archives",
            "label": "Archives",
            "description": "Have log2timeline process files inside archives - True or False",
            "type": "boolean",
            "required": False,
        },
    ],
}


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def log2timeline(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run log2timeline on input files.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []
    temp_dir = None
    output_file = create_output_file(
        output_path, uuid_as_filename=True, add_extension="plaso"
    )
    status_file = create_output_file(
        output_path, uuid_as_filename=True, add_extension="status"
    )

    command = [
        "log2timeline.py",
        "--quiet",
        "--unattended",
        "--partitions",
        "all",
        "--status-view",
        "file",
        "--status-view-file",
        status_file.path,
        "--storage-file",
        output_file.path,
    ]

    if task_config and task_config.get("artifacts"):
        command.extend(["--artifact_filters", ",".join(task_config["artifacts"])])

    if task_config and task_config.get("parsers"):
        command.extend(["--parsers", ",".join(task_config["parsers"])])

    # For task result metadata
    command_string = " ".join(command)

    if len(input_files) > 1:
        # Create temporary directory and hard link files for processing
        temp_dir = os.path.join(output_path, uuid4().hex)
        os.mkdir(temp_dir)
        for input_file in input_files:
            filename = os.path.basename(input_file.get("path"))
            os.link(input_file.get("path"), f"{temp_dir}/{filename}")

        # Add the data to be processed
        command.append(temp_dir)
    else:
        command.append(input_files[0].get("path"))

    process = subprocess.Popen(command)
    while process.poll() is None:
        if not os.path.exists(status_file.path):
            continue
        with open(status_file.path, "r") as f:
            status_dict = log2timeline_status_to_dict(f.read())
            self.send_event("task-progress", data=status_dict)
        time.sleep(2)

    # TODO: File feature request in Plaso to get these methods public.
    pinfo = pinfo_tool.PinfoTool()
    storage_reader = pinfo._GetStorageReader(output_file.path)
    storage_version = storage_reader.GetFormatVersion()
    storage_counter = pinfo._CalculateStorageCounters(storage_reader).get("parsers", {})

    if temp_dir:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    output_files.append(output_file.to_dict())

    if not output_files:
        raise RuntimeError("log2timeline didn't create any output files")

    return task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=command_string,
        meta={
            "plaso_version": str(plaso_version),
            "plaso_storage_version": str(storage_version),
            "event_counters": storage_counter,
        },
    )
