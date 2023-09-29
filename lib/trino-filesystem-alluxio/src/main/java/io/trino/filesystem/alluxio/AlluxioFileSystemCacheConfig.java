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
package io.trino.filesystem.alluxio;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class AlluxioFileSystemCacheConfig
{
    private String cacheDirectories;
    private String maxCacheSize;

    @NotNull
    public String getCacheDirectories()
    {
        return cacheDirectories;
    }

    @Config("fs.cache.directories")
    @ConfigDescription("Base directory to cache data. A comma-separated list of directories to use")
    public AlluxioFileSystemCacheConfig setCacheDirectories(String value)
    {
        this.cacheDirectories = value;
        return this;
    }

    @NotNull
    public String getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("fs.cache.max-size")
    @ConfigDescription("The maximum cache size available for cache. A comma-separated list of sizes if supplying several cache directories")
    public AlluxioFileSystemCacheConfig setMaxCacheSize(String maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }
}
