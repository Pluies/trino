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

import alluxio.conf.AlluxioConfiguration;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.TrackingFileSystemFactory.OperationType;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_LAST_MODIFIED;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.filesystem.alluxio.TestingAlluxioFileSystemCache.OperationType.CACHE_READ;
import static io.trino.filesystem.alluxio.TestingAlluxioFileSystemCache.OperationType.EXTERNAL_READ;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(Lifecycle.PER_CLASS)
public class TestAlluxioCacheFileSystemAccessOperations
{
    private TrackingFileSystemFactory trackingFileSystemFactory;
    private TestingAlluxioFileSystemCache alluxioCache;
    private CacheFileSystem fileSystem;

    private Path tempDirectory;

    private static final int CACHE_SIZE = 1024;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test");
        Path cacheDirectory = Files.createDirectory(tempDirectory.resolve("cache"));

        AlluxioFileSystemCacheConfig configuration = new AlluxioFileSystemCacheConfig()
                .setCacheDirectories(cacheDirectory.toAbsolutePath().toString())
                .setMaxCacheSize(Integer.toString(CACHE_SIZE));
        AlluxioConfiguration alluxioConfiguration = AlluxioFileSystemCacheModule.getAlluxioConfiguration(configuration);

        trackingFileSystemFactory = new TrackingFileSystemFactory(new MemoryFileSystemFactory());
        alluxioCache = new TestingAlluxioFileSystemCache(alluxioConfiguration);
        fileSystem = new CacheFileSystem(trackingFileSystemFactory.create(ConnectorIdentity.ofUser("hello")),
                alluxioCache);
    }

    @AfterAll
    public void tearDown()
    {
        trackingFileSystemFactory = null;
        fileSystem = null;
        tempDirectory.toFile().delete();
        tempDirectory = null;
    }

    @Test
    public void testCache()
            throws IOException
    {
        Location location = getRootLocation().appendPath("hello");
        byte[] content = "hello world".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        assertReadOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(location, INPUT_FILE_GET_LENGTH))
                        .add(new FileOperation(location, INPUT_FILE_LAST_MODIFIED))
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation(location, EXTERNAL_READ))
                        .build());
        assertReadOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, INPUT_FILE_GET_LENGTH))
                        .add(new FileOperation(location, INPUT_FILE_LAST_MODIFIED))
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation(location, CACHE_READ))
                        .build());

        byte[] modifiedContent = "modified content".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).createOrOverwrite()) {
            output.write(modifiedContent);
        }

        assertReadOperations(location, modifiedContent,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(location, INPUT_FILE_GET_LENGTH))
                        .add(new FileOperation(location, INPUT_FILE_LAST_MODIFIED))
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation(location, EXTERNAL_READ))
                        .build());
    }

    private Location createFile(String name, int size)
            throws IOException
    {
        Location location = getRootLocation().appendPath(name);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write("a".repeat(size).getBytes(StandardCharsets.UTF_8));
        }
        return location;
    }

    @Test
    public void testCacheInvalidation()
            throws IOException
    {
        int cacheSize = (int) (0.9 * CACHE_SIZE);
        Location aLocation = createFile("a", cacheSize);
        Location bLocation = createFile("b", cacheSize);
        Location cLocation = createFile("c", cacheSize / 2);
        Location dLocation = createFile("d", cacheSize / 2);

        assertUnCachedRead(aLocation);
        assertCachedRead(aLocation);
        assertUnCachedRead(bLocation);
        assertUnCachedRead(aLocation);
        assertCachedRead(aLocation);
        assertCachedRead(aLocation);
        assertUnCachedRead(bLocation);
        assertCachedRead(bLocation);

        assertUnCachedRead(cLocation);
        assertUnCachedRead(dLocation);
        assertCachedRead(cLocation);
        assertCachedRead(dLocation);

        assertUnCachedRead(bLocation);
        assertCachedRead(bLocation);
        assertUnCachedRead(cLocation);
        assertUnCachedRead(dLocation);
    }

    private Location getRootLocation()
    {
        return Location.of("memory://");
    }

    private void assertCachedRead(Location location)
            throws IOException
    {
        assertReadOperations(location,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, INPUT_FILE_GET_LENGTH))
                        .add(new FileOperation(location, INPUT_FILE_LAST_MODIFIED))
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation(location, CACHE_READ))
                        .build());
    }

    private void assertUnCachedRead(Location location)
            throws IOException
    {
        assertReadOperations(location,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(location, INPUT_FILE_GET_LENGTH))
                        .add(new FileOperation(location, INPUT_FILE_LAST_MODIFIED))
                        .build(),
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation(location, EXTERNAL_READ))
                        .build());
    }

    private void assertReadOperations(Location location, Multiset<FileOperation> fileOperations, Multiset<CacheOperation> cacheOperations)
            throws IOException
    {
        TrinoInputFile file = fileSystem.newInputFile(location);
        int length = (int) file.length();
        trackingFileSystemFactory.reset();
        alluxioCache.reset();
        try (TrinoInput input = file.newInput()) {
            input.readFully(0, length);
        }
        assertMultisetsEqual(getOperations(), fileOperations);
        assertMultisetsEqual(getCacheOperations(), cacheOperations);
    }

    private void assertReadOperations(Location location, byte[] content, Multiset<FileOperation> fileOperations, Multiset<CacheOperation> cacheOperations)
            throws IOException
    {
        TrinoInputFile file = fileSystem.newInputFile(location);
        int length = (int) file.length();
        trackingFileSystemFactory.reset();
        alluxioCache.reset();
        try (TrinoInput input = file.newInput()) {
            assertThat(input.readFully(0, length)).isEqualTo(Slices.wrappedBuffer(content));
        }
        assertMultisetsEqual(fileOperations, getOperations());
        assertMultisetsEqual(cacheOperations, getCacheOperations());
    }

    private Multiset<FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), new FileOperation(
                        entry.getKey().location(),
                        entry.getKey().operationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return alluxioCache.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), new CacheOperation(
                        entry.getKey().location(),
                        entry.getKey().type())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(Location path, OperationType operationType) {}

    private record CacheOperation(Location path, TestingAlluxioFileSystemCache.OperationType operationType) {}
}
