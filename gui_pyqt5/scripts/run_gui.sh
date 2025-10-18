#!/bin/bash
cd "$(dirname "$0")/.."
source ../venv/bin/activate  # nếu bạn đang dùng virtualenv
python3 GUI.py
