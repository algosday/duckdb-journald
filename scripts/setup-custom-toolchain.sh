#!/usr/bin/env bash
set -eox pipefail
apt install -y autoconf libsystemd-dev
python3 -m pip install jinja2