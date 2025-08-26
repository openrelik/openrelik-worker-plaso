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
import os

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import (
    create_task_result,
    get_input_files,
)
from plaso import __version__ as plaso_version
from plaso.cli import pinfo_tool
from plaso.cli.extraction_tool import ExtractionTool
from plaso.parsers import manager

from .app import celery
from .utils import is_ewf_files, log2timeline_status_to_dict

# Get all Plaso parser names to use for user config form.
parser_manager = manager.ParsersManager()
parser_names = {parser for parser, _ in parser_manager.GetParsersInformation()}
for plugin in parser_manager.GetNamesOfParsersWithPlugins():
    for parser, _ in parser_manager.GetParserPluginsInformation(parser_filter_expression=plugin):
        parser_names.add(f"{plugin}/{parser}")

# Get all Plaso supported archive types for user config form.
# TODO(rbdebeer) - fix this when public function has been added
# to plaso.cli.helpers.archives
archive_names_dict = ExtractionTool._SUPPORTED_ARCHIVE_TYPES
archive_names = list(archive_names_dict.keys())

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-plaso.tasks.log2timeline"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Plaso Log2Timeline",
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
            "name": "archives",
            "label": "Archives",
            "description": "Select one or more Plaso archive types. Files inside these archive types will be processed.",
            "type": "autocomplete",
            "items": archive_names,
            "required": False,
        },
        {
            "name": "Yara rules",
            "label": "Yara rules",
            "description": "Add Yara rules to tag files with.",
            "type": "textarea",
            "required": False,
        },
        {
            "name": "output_file_name",
            "label": "Output file name",
            "description": "Custom name for the output Plaso file (without extension).",
            "type": "text",
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

    # Determine output file name from task_config if provided
    custom_name = None
    if task_config and task_config.get("output_file_name"):
        custom_name = task_config["output_file_name"].strip()
        # Remove any extension, then add .plaso
        custom_name = os.path.splitext(custom_name)[0]
        if not custom_name.lower().endswith(".plaso"):
            custom_name = f"{custom_name}.plaso"

    if len(input_files) == 1:
        if custom_name:
            display_name = custom_name
            output_file = create_output_file(
                output_path,
                display_name=display_name,
                data_type="plaso:log2timeline:plaso_storage",
            )
        else:
            display_name = f"{input_files[0].get('display_name')}"
            output_file = create_output_file(
                output_path,
                display_name=f"{display_name}.plaso",
                data_type="plaso:log2timeline:plaso_storage",
            )
    else:
        if custom_name:
            display_name = custom_name
            output_file = create_output_file(
                output_path,
                display_name=display_name,
                data_type="plaso:log2timeline:plaso_storage",
            )
        else:
            output_file = create_output_file(
                output_path,
                extension="plaso",
                data_type="plaso:log2timeline:plaso_storage",
            )
    status_file = create_output_file(output_path, extension="status")

    command = [
        "log2timeline.py",
        "--quiet",
        "--unattended",
        "--partitions",
        "all",
        "--volumes",
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

    if task_config and task_config.get("archives"):
        command.extend(["--archives", ",".join(task_config["archives"])])

    if task_config and task_config.get("Yara rules"):
        yara_rules_file = create_output_file(
            output_path,
            extension="yara",
            data_type="plaso:log2timeline:yara_rules",
        )
        with open(yara_rules_file.path, "w") as f:
            f.write(task_config["Yara rules"])
        command.extend(["--yara_rules", yara_rules_file.path])

    command_string = " ".join(command)

    if len(input_files) > 1:
        # Create temporary directory and hard link files for processing
        temp_dir = os.path.join(output_path, uuid4().hex)
        os.mkdir(temp_dir)

        # Create hard links for each input file in the temporary directory
        # If the files are EWF files, use a base name for the links to preserve the EWF structure.
        # Note: This only works if all input files are EWF files.
        if is_ewf_files(input_files):
            base_name = uuid4().hex
            for input_file in input_files:
                original_path = input_file.get("path")
                original_ext = os.path.splitext(original_path)[1]
                new_filename = f"{base_name}{original_ext}".lower()
                link_path = os.path.join(temp_dir, new_filename)
                os.link(original_path, link_path)
            # Use the first file's base name to use in Plaso.
            e01_file = os.path.join(temp_dir, f"{base_name}.e01")
            if not os.path.exists(e01_file):
                raise RuntimeError(f"Expected EWF file {e01_file} does not exist.")
            command.append(e01_file)
        else:
            for input_file in input_files:
                filename = os.path.basename(input_file.get("path"))
                os.link(input_file.get("path"), f"{temp_dir}/{filename}")
            command.append(temp_dir)
    else:
        command.append(input_files[0].get("path"))

    # Send initial event to indicate task has started
    self.send_event("task-progress", data={})

    process = subprocess.Popen(command)
    while process.poll() is None:
        if not os.path.exists(status_file.path):
            continue
        with open(status_file.path, "r") as f:
            status_dict = log2timeline_status_to_dict(f.read())
            self.send_event("task-progress", data=status_dict)
        time.sleep(3)

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

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=command_string,
        meta={
            "plaso_version": str(plaso_version),
            "plaso_storage_version": str(storage_version),
            "event_counters": storage_counter,
        },
    )
