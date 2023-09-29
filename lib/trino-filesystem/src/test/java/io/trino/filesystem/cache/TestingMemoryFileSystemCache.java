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
package io.trino.filesystem.cache;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryFileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TestingMemoryFileSystemCache
        implements TrinoFileSystemCache
{
    private final MemoryFileSystem memoryCache;

    public TestingMemoryFileSystemCache()
    {
        this.memoryCache = new MemoryFileSystem();
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        Location cacheLocation = Location.of("memory:///" + key.replace("memory:///", "") + key.hashCode());
        TrinoInputFile cacheKey = memoryCache.newInputFile(cacheLocation);
        if (!cacheKey.exists()) {
            try (OutputStream output = memoryCache.newOutputFile(cacheLocation).create();
                    InputStream input = delegate.newStream()) {
                input.transferTo(output);
            }
        }
        return cacheKey.newInput();
    }
}
