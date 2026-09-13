#!/usr/bin/env bash
# Copyright 2026 The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

NOTEBOOK_INPUT="${NOTEBOOK_INPUT:?NOTEBOOK_INPUT must be set}"
NOTEBOOK_OUTPUT="${NOTEBOOK_OUTPUT:-/tmp/output.ipynb}"
PAPERMILL_TIMEOUT="${PAPERMILL_TIMEOUT:-900}"

echo "=================================================================="
echo "Running Notebook: ${NOTEBOOK_INPUT}"
echo "Output Path:      ${NOTEBOOK_OUTPUT}"
echo "Execution Timeout:${PAPERMILL_TIMEOUT}s"
echo "=================================================================="

# Ensure output directory exists
mkdir -p "$(dirname "${NOTEBOOK_OUTPUT}")"

# Install ipykernel kernel spec if not present using uv environment
uv run python3 -m ipykernel install --user --name python3 --display-name "Python 3"

# Execute notebook via papermill inside uv environment
uv run papermill \
    "${NOTEBOOK_INPUT}" \
    "${NOTEBOOK_OUTPUT}" \
    --kernel python3 \
    --execution-timeout "${PAPERMILL_TIMEOUT}" \
    --stdout-file /dev/stdout \
    --stderr-file /dev/stderr \
    --log-output

echo "✓ Notebook ${NOTEBOOK_INPUT} passed successfully!"
