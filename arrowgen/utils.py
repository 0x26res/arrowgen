import importlib
import subprocess

import typing


def run_command(command: typing.List[str]):
    results = subprocess.run(command, text=True, capture_output=True)
    if results.returncode != 0:
        raise RuntimeError(f"[{' '.join(command)}] Failed with {results.stderr}")


def load_python_file(python_file: str):
    spec = importlib.util.spec_from_file_location("module.name", python_file)
    loaded_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded_module)
    return loaded_module
