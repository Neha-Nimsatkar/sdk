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

from kubeflow.spark import Driver, Executor, SparkClient

IMAGE = "spark-s3-iceberg:4.0.4"

CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.type": "hadoop",
    "spark.sql.catalog.lake.warehouse": "s3a://mlpipeline/warehouse",
    "spark.hadoop.fs.s3a.endpoint": "http://seaweedfs-s3.seaweedfs.svc.cluster.local:8333",
    "spark.hadoop.fs.s3a.access.key": "dummy",
    "spark.hadoop.fs.s3a.secret.key": "dummy",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
}


def get_spark():
    client = SparkClient()
    spark = client.connect(
        driver=Driver(image=IMAGE, resources={"cpu": "1", "memory": "512Mi"}),
        executor=Executor(num_instances=1, resources_per_executor={"cpu": "1", "memory": "512Mi"}),
        spark_conf=CONF,
        timeout=300,
    )
    return client, spark
