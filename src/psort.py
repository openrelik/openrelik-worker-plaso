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

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery
from .utils import log2timeline_status_to_dict

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-plaso.tasks.psort"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Plaso Psort CSV",
    "description": "Process Plaso storage files",
}


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
    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []

    for input_file in input_files:
        output_file = create_output_file(
            output_path,
            display_name=f"{input_file.get('display_name')}.csv",
            data_type="plaso:psort:csv",
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
            input_file.get("path"),
        ]
        command_string = " ".join(command[:5])

        # Send initial status event to indicate task start
        self.send_event("task-progress", data={})

        process = subprocess.Popen(command)
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

    output_files.append(output_file.to_dict())

    if not output_files:
        raise RuntimeError("psort didn't create any output files")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=command_string,
    )
