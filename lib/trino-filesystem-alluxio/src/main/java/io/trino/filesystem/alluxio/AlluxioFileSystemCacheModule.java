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

import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.cache.NodeProvider;
import io.trino.filesystem.cache.TrinoFileSystemCache;

import java.io.IOException;
import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class AlluxioFileSystemCacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AlluxioFileSystemCacheConfig.class);

        newMapBinder(binder, String.class, TrinoFileSystemCache.class).addBinding("alluxio").to(AlluxioFileSystemCache.class).in(SINGLETON);
        binder.bind(NodeProvider.class).to(ConsistentHashingNodeProvider.class).in(SINGLETON);

        Properties metricProps = new Properties();
        metricProps.put("sink.jmx.class", "alluxio.metrics.sink.JmxSink");
        metricProps.put("sink.jmx.domain", "org.alluxio");
        MetricsSystem.startSinksFromConfig(new MetricsConfig(metricProps));
    }

    @Inject
    @Provides
    @Singleton
    public static AlluxioConfiguration getAlluxioConfiguration(AlluxioFileSystemCacheConfig config)
    {
        AlluxioProperties alluxioProperties = new AlluxioProperties();
        alluxioProperties.set(PropertyKey.USER_CLIENT_CACHE_ENABLED, true);
        alluxioProperties.set(PropertyKey.USER_CLIENT_CACHE_DIRS, config.getCacheDirectories());
        alluxioProperties.set(PropertyKey.USER_CLIENT_CACHE_SIZE, config.getMaxCacheSize());
        alluxioProperties.set(PropertyKey.USER_CLIENT_CACHE_SHADOW_ENABLED, true);
        return new InstancedConfiguration(alluxioProperties);
    }

    @Inject
    @Provides
    @Singleton
    public static CacheManager getCacheManager(AlluxioConfiguration alluxioConfiguration)
    {
        try {
            return CacheManager.Factory.get(alluxioConfiguration);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
