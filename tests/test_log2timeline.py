"""tests for log2timeline."""

import ast
from pathlib import Path


LOG2TIMELINE_PATH = (
    Path(__file__).resolve().parent.parent / "src" / "log2timeline.py"
)


def _task_metadata_node():
    tree = ast.parse(LOG2TIMELINE_PATH.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "TASK_METADATA":
                    return node.value
    raise AssertionError("TASK_METADATA assignment not found in log2timeline.py")


def test_register_in_db_switch_declared_in_task_metadata():
    """The user-facing switch must be wired into TASK_METADATA so the UI
    renders it as a configuration option."""
    metadata_dict = _task_metadata_node()
    assert isinstance(metadata_dict, ast.Dict)

    # Find the task_config list value.
    task_config_value = None
    for k, v in zip(metadata_dict.keys, metadata_dict.values):
        if isinstance(k, ast.Constant) and k.value == "task_config":
            task_config_value = v
            break
    assert task_config_value is not None, "task_config key missing from TASK_METADATA"
    assert isinstance(task_config_value, ast.List)

    names = []
    for entry in task_config_value.elts:
        assert isinstance(entry, ast.Dict)
        for k, v in zip(entry.keys, entry.values):
            if (
                isinstance(k, ast.Constant)
                and k.value == "name"
                and isinstance(v, ast.Constant)
            ):
                names.append(v.value)
    assert "register_in_db" in names, (
        "register_in_db switch missing from log2timeline TASK_METADATA"
    )


def test_register_in_db_threaded_through_to_create_output_file():
    """Both create_output_file calls in log2timeline must pass register_in_db
    so single-input and multi-input runs both honor the user's choice."""
    source = LOG2TIMELINE_PATH.read_text()
    # Crude but effective: every create_output_file invocation that produces
    # the user-visible .plaso storage file should mention register_in_db.
    storage_calls = source.count('data_type="plaso:log2timeline:plaso_storage"')
    register_passes = source.count("register_in_db=register_in_db")
    assert storage_calls >= 2, (
        "expected single-input and multi-input branches to both create the "
        "plaso storage OutputFile"
    )
    assert register_passes >= storage_calls, (
        "every plaso_storage create_output_file call must thread register_in_db"
    )


def test_single_input_threads_upstream_original_path():
    """Single-input runs must forward the upstream upload's original_path
    onto the .plaso OutputFile so downstream tasks like the S3 exporter can
    derive object keys from the user's uploaded filename rather than this
    worker's UUID-based on-disk name."""
    source = LOG2TIMELINE_PATH.read_text()
    # The single-input branch should pass original_path explicitly, derived
    # from the upstream input's original_path with a fallback to its path.
    assert 'original_path=upstream_original' in source, (
        "single-input branch must thread the upstream original_path "
        "into create_output_file"
    )
    assert (
        'input_files[0].get("original_path")' in source
        and 'input_files[0].get("path")' in source
    ), "single-input branch must derive upstream_original with a path fallback"
