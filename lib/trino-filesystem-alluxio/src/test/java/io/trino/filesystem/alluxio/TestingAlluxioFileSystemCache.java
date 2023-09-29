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

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.metrics.MetricKey;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TestingAlluxioFileSystemCache
        extends AlluxioFileSystemCache
{
    public enum OperationType
    {
        CACHE_READ,
        EXTERNAL_READ,
    }

    public record OperationContext(OperationType type, Location location) {}

    private final Map<OperationContext, Integer> operationCounts = new ConcurrentHashMap<>();
    private AtomicInteger cacheGeneration = new AtomicInteger(0);

    public TestingAlluxioFileSystemCache(AlluxioConfiguration alluxioConfiguration)
    {
        super(AlluxioFileSystemCacheModule.getCacheManager(alluxioConfiguration), alluxioConfiguration);
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        return super.cacheInput(delegate, key + cacheGeneration.get());
    }

    public void reset()
    {
        operationCounts.clear();
    }

    public void clear()
    {
        this.cacheGeneration.incrementAndGet();
    }

    public Map<OperationContext, Integer> getOperationCounts()
    {
        return ImmutableMap.copyOf(operationCounts);
    }

    @Override
    protected URIStatus uriStatus(TrinoInputFile file, String key)
            throws IOException
    {
        URIStatus status = super.uriStatus(file, key);
        Location path = file.location();
        return new URIStatus(status.getFileInfo(), status.getCacheContext())
        {
            @Override
            public CacheContext getCacheContext()
            {
                CacheContext cacheContext = super.getCacheContext();
                return new CacheContext()
                {
                    @Override
                    public void incrementCounter(String name, StatsUnit unit, long value)
                    {
                        if (MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getMetricName().equals(name)) {
                            operationCounts.merge(new OperationContext(OperationType.CACHE_READ, path), 1, Math::addExact);
                        }
                        else if (MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getMetricName().equals(name)) {
                            operationCounts.merge(new OperationContext(OperationType.EXTERNAL_READ, path), 1, Math::addExact);
                        }
                    }
                }
                        .setCacheQuota(cacheContext.getCacheQuota())
                        .setCacheScope(cacheContext.getCacheScope())
                        .setHiveCacheContext(cacheContext.getHiveCacheContext())
                        .setTemporary(cacheContext.isTemporary())
                        .setCacheIdentifier(cacheContext.getCacheIdentifier());
            }
        };
    }
}
