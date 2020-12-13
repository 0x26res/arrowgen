import subprocess


def run_command(command):
    results = subprocess.run(command, text=True, capture_output=True)
    if results.returncode != 0:
        raise RuntimeError(f"Failed with {results.stderr}")