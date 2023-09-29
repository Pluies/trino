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
package io.trino.plugin.deltalake.cache;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.alluxio.AlluxioFileSystemCache;
import io.trino.filesystem.cache.TrinoFileSystemCache;

import java.io.IOException;

public class DeltaLakeAlluxioFileSystemCache
        implements TrinoFileSystemCache
{
    private final AlluxioFileSystemCache cache;

    @Inject
    public DeltaLakeAlluxioFileSystemCache(AlluxioFileSystemCache cache)
    {
        this.cache = cache;
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate)
            throws IOException
    {
        // TODO: Enable caching of parquet checkpoint files once the CheckpointEntryIterator is always closed
        if (!delegate.location().path().contains("/_delta_log/")) {
            return cache.cacheInput(delegate, delegate.location().path());
        }
        return delegate.newInput();
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        return cache.cacheInput(delegate, key);
    }
}
