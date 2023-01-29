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
package io.trino.filesystem.hdfs.cache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.annotation.Nullable;

public class CachingFileSystemConfig
{
    private boolean cacheEnabled;
    private String cacheDir = "/tmp/alluxio_cache"; // Alluxio's default cache dir
    private String maxCacheSize = "512MB"; // Alluxio's default cache size

    public boolean isCacheEnabled()
    {
        return cacheEnabled;
    }

    @Config("cache.enabled")
    @ConfigDescription("Cache data file to workers' local storage")
    public CachingFileSystemConfig setCacheEnabled(boolean value)
    {
        this.cacheEnabled = value;
        return this;
    }

    @Nullable
    public String getCacheDirectories()
    {
        return cacheDir;
    }

    @Config("cache.directories")
    @ConfigDescription("Base directory to cache data. Can be a comma-separated list of directories to use several disks.")
    public CachingFileSystemConfig setCacheDirectories(String value)
    {
        this.cacheDir = value;
        return this;
    }

    public String getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("cache.max-cache-size")
    @ConfigDescription("The maximum cache size available for cache. Can be a comma-separated list of sizes if supplying several cache directories.")
    public CachingFileSystemConfig setMaxCacheSize(String maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }
}
