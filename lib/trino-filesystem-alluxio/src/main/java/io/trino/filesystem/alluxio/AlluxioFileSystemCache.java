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
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.conf.AlluxioConfiguration;
import alluxio.wire.FileInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.TrinoFileSystemCache;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AlluxioFileSystemCache
        implements TrinoFileSystemCache
{
    private final CacheManager cacheManager;
    private final AlluxioConfiguration alluxioConf;
    private final HashFunction hashFunction = Hashing.murmur3_128();

    @Inject
    public AlluxioFileSystemCache(CacheManager cacheManager, AlluxioConfiguration alluxioConf)
    {
        this.cacheManager = cacheManager;
        this.alluxioConf = alluxioConf;
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        URIStatus uriStatus = uriStatus(delegate, key);
        LocalCacheFileInStream localCacheFileInStream = new LocalCacheFileInStream(uriStatus, (uri) -> new AlluxioTrinoInputStream(delegate.newStream()), cacheManager, alluxioConf);
        return new AlluxioInput(localCacheFileInStream, delegate);
    }

    @VisibleForTesting
    protected URIStatus uriStatus(TrinoInputFile file, String key)
            throws IOException
    {
        String path = file.location().toString();

        FileInfo info = new FileInfo()
                .setPath(path)
                .setFolder(false)
                .setLength(file.length());
        String cacheIdentifier = hashFunction.hashString(key, UTF_8).toString();
        return new URIStatus(info, CacheContext.defaults().setCacheIdentifier(cacheIdentifier));
    }
}
