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
package io.trino.filesystem.manager;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.alluxio.AlluxioFileSystemCacheModule;
import io.trino.filesystem.cache.CacheFileSystemFactory;
import io.trino.filesystem.cache.NodeProvider;
import io.trino.filesystem.cache.NoneNodeProvider;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemModule;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.hdfs.s3.HiveS3Module;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;

public class FileSystemModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        FileSystemConfig config = buildConfigObject(FileSystemConfig.class);

        binder.bind(HdfsFileSystemFactoryHolder.class).in(SINGLETON);

        if (config.isHadoopEnabled()) {
            install(new HdfsFileSystemModule());
        }

        var factories = newMapBinder(binder, String.class, TrinoFileSystemFactory.class);

        if (config.isNativeS3Enabled()) {
            install(new S3FileSystemModule());
            factories.addBinding("s3").to(S3FileSystemFactory.class);
            factories.addBinding("s3a").to(S3FileSystemFactory.class);
            factories.addBinding("s3n").to(S3FileSystemFactory.class);
        }
        else {
            install(new HiveS3Module());
        }

        newMapBinder(binder, String.class, TrinoFileSystemCache.class);
        if (config.getCacheType() != null) {
            install(conditionalModule(
                    FileSystemConfig.class,
                    cache -> "alluxio".equalsIgnoreCase(cache.getCacheType()),
                    new AlluxioFileSystemCacheModule()));
        }
        else {
            binder.bind(NodeProvider.class).to(NoneNodeProvider.class).in(SINGLETON);
        }
    }

    @Provides
    @Singleton
    public TrinoFileSystemFactory createFileSystemFactory(
            HdfsFileSystemFactoryHolder hdfsFileSystemFactory,
            Map<String, TrinoFileSystemFactory> factories,
            Optional<TrinoFileSystemCache> fileSystemCache,
            Tracer tracer)
    {
        TrinoFileSystemFactory delegate = new SwitchingFileSystemFactory(hdfsFileSystemFactory.value(), factories);
        return new TracingFileSystemFactory(tracer,
                fileSystemCache.map(cache -> (TrinoFileSystemFactory) new CacheFileSystemFactory(delegate, cache)).orElse(delegate));
    }

    @Provides
    @Singleton
    public Optional<TrinoFileSystemCache> createFileSystemCache(FileSystemConfig config, Map<String, TrinoFileSystemCache> caches)
    {
        return Optional.ofNullable(caches.get(config.getCacheType()));
    }

    public static class HdfsFileSystemFactoryHolder
    {
        @Inject(optional = true)
        private HdfsFileSystemFactory hdfsFileSystemFactory;

        public Optional<TrinoFileSystemFactory> value()
        {
            return Optional.ofNullable(hdfsFileSystemFactory);
        }
    }
}
