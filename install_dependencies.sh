#!/usr/bin/bash

source bin/activate
pip3 install watchdog construct aioquic loguru ruff textual
cd MD5
python3 setup.py install
deactivate

