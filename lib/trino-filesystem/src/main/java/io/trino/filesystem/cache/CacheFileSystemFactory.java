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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

public class CacheFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final TrinoFileSystemFactory delegate;
    private final TrinoFileSystemCache cache;

    public CacheFileSystemFactory(TrinoFileSystemFactory delegate, TrinoFileSystemCache cache)
    {
        this.delegate = delegate;
        this.cache = cache;
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new CacheFileSystem(delegate.create(identity), cache);
    }
}
