# Copyright The Kubeflow Authors.
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

"""Push enriched data into the Trainer data cache.

The cache head accepts Arrow record batches over Arrow Flight (``do_put`` on the
``push`` path) and splits them into one contiguous row range per cache worker.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.flight as flight

DEFAULT_ENDPOINT = "grpc://localhost:50051"
PUSH_PATH = "push"


def to_arrow(data) -> pa.Table:
    """Convert a Spark DataFrame, pandas DataFrame or Arrow table to an Arrow table."""
    if isinstance(data, pa.Table):
        return data
    if hasattr(data, "toArrow"):  # Spark 4 DataFrame
        return data.toArrow()
    if hasattr(data, "toPandas"):  # older Spark DataFrame
        return pa.Table.from_pandas(data.toPandas(), preserve_index=False)
    return pa.Table.from_pandas(data, preserve_index=False)  # pandas DataFrame


def push_to_data_cache(
    data,
    endpoint: str = DEFAULT_ENDPOINT,
    *,
    schema: pa.Schema | None = None,
    chunk_rows: int = 500,
    timeout: float = 120,
) -> int:
    """Send ``data`` to the cache head and return the number of rows pushed.

    Args:
        data: A Spark DataFrame, pandas DataFrame or ``pyarrow.Table``.
        endpoint: Flight address of the cache head, for example
            ``grpc://localhost:50051`` through a port-forward to ``<job>-cache-0``.
        schema: Optional schema to cast the data to before sending.
        chunk_rows: Maximum rows per record batch.
        timeout: Seconds before the push fails instead of hanging.

    Raises:
        ValueError: If there is no data to push.
        pyarrow.flight.FlightError: If the cache head rejects or cannot receive the data.
    """
    table = to_arrow(data)
    if schema is not None:
        table = table.cast(schema)
    if table.num_rows == 0:
        raise ValueError("Nothing to push: the data has no rows.")

    client = flight.connect(endpoint)
    options = flight.FlightCallOptions(timeout=timeout)
    writer, _ = client.do_put(flight.FlightDescriptor.for_path(PUSH_PATH), table.schema, options)
    writer.write_table(table, max_chunksize=chunk_rows)
    writer.close()
    return table.num_rows
