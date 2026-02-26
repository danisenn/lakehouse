#!/usr/bin/env python3
"""
Remote Benchmark Runner wrapper script.

This script executes the batch testing (test/src/benchmark_runner.py) over SSH
on a remote server within a Docker container and copies the resulting JSON 
report back to the local Mac.
"""
import os
import sys
import argparse
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Set ROOT_DIR to the base of the lakehouse project locally
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT_DIR / ".env")

def main():
    parser = argparse.ArgumentParser(description="Run batch testing on a remote server via SSH and save results locally.")
    parser.add_argument("--host", required=True, help="SSH connection string (e.g., user@hostname)")
    parser.add_argument("--remote-dir", required=True, help="Absolute path to the lakehouse repository on the remote server (e.g., /home/user/lakehouse-service)")
    parser.add_argument("--table", help="Table name to benchmark")
    parser.add_argument("--all", action="store_true", help="Process all tables in the schema")
    parser.add_argument("--schema", help="Source schema (passed to benchmark_runner.py)")
    parser.add_argument("--target-schema", help="Target schema (passed to benchmark_runner.py)")
    parser.add_argument("--limit", type=int, help="Row limit (passed to benchmark_runner.py)")
    parser.add_argument("--skip-generation", action="store_true", help="Skip variant generation")
    
    # Execution mode (Native vs Docker)
    parser.add_argument("--python-cmd", default="python3", help="If running natively without Docker, the python command to use.")
    parser.add_argument("--docker-image", help="If provided, runs the script inside a fresh Docker container of this image name with volume mounts.")
    
    parser.add_argument("--ssh-key", help="Path to a specific SSH private key file (optional)")
    
    args = parser.parse_args()
    
    # Check for SSH Password for sshpass automation
    ssh_password = os.getenv("REMOTE_SSH_PASSWORD")
    if ssh_password:
        os.environ["SSHPASS"] = ssh_password
    
    # Validate arguments
    if not args.table and not args.all:
        parser.error("Either --table or --all must be specified")
        
    # 1. Construct the internal python command
    internal_script_path = "test/src/benchmark_runner.py"
    
    python_cmd = [
        "python",  # inside docker it's usually just python
        internal_script_path
    ]
    if args.table:
        python_cmd.extend(["--table", args.table])
    if args.all:
        python_cmd.append("--all")
    if args.schema:
        python_cmd.extend(["--schema", args.schema])
    if args.target_schema:
        python_cmd.extend(["--target-schema", args.target_schema])
    if args.limit:
        python_cmd.extend(["--limit", str(args.limit)])
    if args.skip_generation:
        python_cmd.append("--skip-generation")
        
    python_cmd_str = " ".join(python_cmd)
    
    # 2. Construct the full SSH command
    env_flags = ""
    for var in ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_REGION", "DREMIO_PORT", "DREMIO_USER", "DREMIO_PASSWORD"]:
        val = os.getenv(var)
        if val:
            env_flags += f"-e {var}='{val}' "

    if args.docker_image:
        # Docker Mode
        run_cmd_str = (
            f"docker run --rm "
            f"--network host "
            f"--env-file {args.remote_dir}/.env "
            f"{env_flags} "
            f"-e DREMIO_HOST=localhost "
            f"-v {args.remote_dir}:/app "
            f"-w /app "
            f"{args.docker_image} "
            f"sh -c 'mkdir -p /app/test/data/benchmarks && {python_cmd[0]} -m pip install -q pandas pyyaml minio pyspark && {python_cmd_str}'"
        )
    else:
        # Native Mode
        run_cmd_str = f"cd {args.remote_dir} && {args.python_cmd} {python_cmd_str}"
    
    print(f"\n=======================================================")
    print(f" Phase 1: Running Batch Testing on Remote Server")
    print(f" Host: {args.host}")
    print(f" Mode: {'Docker' if args.docker_image else 'Native'}")
    print(f" Auto-Auth: {'Yes (sshpass)' if ssh_password else 'No (Prompt)'}")
    print(f"=======================================================\n")
    
    ssh_base = ["sshpass", "-e", "ssh"] if ssh_password else ["ssh"]
    
    if args.ssh_key:
        ssh_base.extend(["-i", args.ssh_key])
    
    ssh_cmd = ssh_base + [
        args.host,
        run_cmd_str
    ]
    
    print(f"Executing: {' '.join(ssh_cmd)}\n")
    
    try:
        subprocess.run(ssh_cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Remote execution failed with exit code {e.returncode}")
        print("Please check your SSH connection, remote directory path, permissions or docker state.")
        if e.returncode == 5 and ssh_password:
            print("[HINT] sshpass returned Error 5: Invalid/incorrect password.")
        sys.exit(1)
        
    # 3. Fetch the results back
    remote_results_dir = f"{args.remote_dir}/test/data/benchmarks"
    local_data_dir = ROOT_DIR / "test" / "data"
    
    # Ensure local directory exists
    local_data_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n=======================================================")
    print(f" Phase 2: Copying Results Locally to Mac")
    print(f"=======================================================\n")
    print(f"Source: {args.host}:{remote_results_dir}")
    print(f"Target: {local_data_dir}\n")
    
    scp_base = ["sshpass", "-e", "scp"] if ssh_password else ["scp"]
    
    if args.ssh_key:
        scp_base.extend(["-i", args.ssh_key])
        
    scp_cmd = scp_base + [
        "-r",
        f"{args.host}:{remote_results_dir}",
        str(local_data_dir)
    ]
    
    try:
        subprocess.run(scp_cmd, check=True)
        print(f"\n[SUCCESS] Results successfully saved locally to:\n -> {local_data_dir}/benchmarks\n")
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Failed to copy results from remote server.")
        sys.exit(1)

if __name__ == "__main__":
    main()
