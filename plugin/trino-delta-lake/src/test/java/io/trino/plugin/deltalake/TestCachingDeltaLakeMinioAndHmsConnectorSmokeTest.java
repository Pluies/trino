/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

/**
 * Delta Lake connector smoke test exercising Hive metastore and MinIO storage with Alluxio caching.
 */
public class TestCachingDeltaLakeMinioAndHmsConnectorSmokeTest
        extends TestDeltaLakeMinioAndHmsConnectorSmokeTest
{
    @Override
    protected Map<String, String> deltaStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "true")
                .put("fs.native-s3.enabled", "false")
                .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("hive.s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .put("hive.s3.path-style-access", "true")
                .put("hive.s3.streaming.part-size", "5MB") //must be at least 5MB according to annotations on io.trino.hdfs.s3.HiveS3Config.getS3StreamingPartSize
                .put("delta.enable-non-concurrent-writes", "true")
                .put("cache.enabled", "true")
                .buildOrThrow();
    }
}
