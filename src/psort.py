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
import subprocess
import time

from celery import signals
from celery.utils.log import get_task_logger
from openrelik_common import telemetry
from openrelik_common.logging import Logger
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files
from plaso.output import manager as output_manager

from .app import celery
from .utils import log2timeline_status_to_dict, process_plaso_cli_logs

# Get the supported psort output formats.
output_formats_available = {
    name for name, _ in output_manager.OutputManager.GetOutputClasses()
}

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-plaso.tasks.psort"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Plaso Psort",
    "description": "Process Plaso storage files",
    "task_config": [
        {
            "name": "output_format",
            "label": "Select output format to use",
            "description": "Select the output format for psort, default will be csv.",
            "type": "select",
            "items": sorted(output_formats_available),
            "required": False,
        },
    ],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def psort(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run psort on input files.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    log_root.bind(workflow_id=workflow_id)
    logger.info(f"Starting {TASK_NAME} for workflow {workflow_id}")

    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []
    command_string = ""

    telemetry.add_attribute_to_current_span("input_files", input_files)
    telemetry.add_attribute_to_current_span("task_config", task_config)
    telemetry.add_attribute_to_current_span("workflow_id", workflow_id)

    # Set output extensions based on chosen task config output format, default is csv
    output_extension = "csv"
    if task_config and task_config.get("output_format"):
        output_extension = task_config["output_format"]

    for input_file in input_files:
        output_file = create_output_file(
            output_path,
            display_name=f"{input_file.get('display_name')}.{output_extension}",
            data_type=f"plaso:psort:{output_extension}",
        )
        status_file = create_output_file(output_path, extension="status")

        command = [
            "psort.py",
            "--quiet",
            "--status-view",
            "file",
            "--additional_fields",
            "yara_match",
            "--status-view-file",
            status_file.path,
            "-w",
            output_file.path,
        ]
        if task_config and task_config.get("output_format"):
            command.extend(["-o", task_config["output_format"]])
        command.append(input_file.get("path"))

        command_string = " ".join(command)

        # Send initial status event to indicate task start
        self.send_event("task-progress", data={})

        logger.info(f"Starting {' '.join(command)}")
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        while process.poll() is None:
            if not os.path.exists(status_file.path):
                continue
            with open(status_file.path, "r") as f:
                status_dict = {}
                try:
                    status_dict = log2timeline_status_to_dict(f.read())
                except:
                    pass
                self.send_event("task-progress", data=status_dict)
            time.sleep(2)
        logger.info(process.stdout.read())
        if process.stderr:
            process_plaso_cli_logs(process.stderr.read(), logger)

    output_files.append(output_file.to_dict())

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=command_string,
    )
