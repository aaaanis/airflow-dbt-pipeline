#!/usr/bin/env python3
"""
Test script to verify the setup of the data engineering showcase.
This script checks if the required components are installed and configured correctly.
"""
import sys
import os
import importlib
import subprocess
from typing import List, Dict


def check_python_package(package_name: str) -> bool:
    """Check if a Python package is installed."""
    try:
        importlib.import_module(package_name)
        print(f"✅ {package_name} is installed")
        return True
    except ImportError:
        print(f"❌ {package_name} is not installed")
        return False


def check_command(command: str) -> bool:
    """Check if a command is available in the system."""
    try:
        subprocess.run(
            command.split(), capture_output=True, check=True, text=True
        )
        print(f"✅ {command.split()[0]} is available")
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        print(f"❌ {command.split()[0]} is not available")
        return False


def check_file_exists(file_path: str) -> bool:
    """Check if a file exists."""
    exists = os.path.isfile(file_path)
    if exists:
        print(f"✅ {file_path} exists")
    else:
        print(f"❌ {file_path} does not exist")
    return exists


def main():
    """Run all checks."""
    print("🔍 Testing Data Engineering Showcase Setup")
    print("=========================================")
    
    # Check Python packages
    required_packages = [
        "airflow",
        "dbt",
        "pandas",
        "psycopg2",
        "requests",
        "sqlalchemy",
    ]
    
    packages_status = {
        package: check_python_package(package) for package in required_packages
    }
    
    # Check commands
    required_commands = [
        "docker --version",
        "docker-compose --version",
        "dbt --version",
    ]
    
    commands_status = {
        command: check_command(command) for command in required_commands
    }
    
    # Check files
    required_files = [
        "docker-compose.yml",
        "dbt/dbt_project.yml",
        "dbt/profiles.yml",
    ]
    
    files_status = {
        file: check_file_exists(file) for file in required_files
    }
    
    # Print summary
    print("\n📊 Summary")
    print("=========")
    
    all_checks_passed = (
        all(packages_status.values())
        and all(commands_status.values())
        and all(files_status.values())
    )
    
    if all_checks_passed:
        print("✅ All checks passed! The setup is complete.")
        return 0
    else:
        print("❌ Some checks failed. Please review the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 