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
package io.trino.filesystem.hdfs;

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.conf.AlluxioConfiguration;
import alluxio.hadoop.AlluxioHdfsInputStream;
import alluxio.hadoop.HdfsFileInputStream;
import alluxio.wire.FileInfo;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.time.Instant;

import static io.trino.filesystem.hdfs.HadoopPaths.hadoopPath;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CachingHdfsInputFile
        implements TrinoInputFile
{
    private final HdfsInputFile inputFile;
    private final HdfsEnvironment environment;
    private final HdfsContext context;
    private final Path file;
    private final CacheManager cacheManager;
    private final AlluxioConfiguration alluxioConf;

    private final FileSystem.Statistics statistics = new FileSystem.Statistics("alluxio");
    private final HashFunction hashFunction = Hashing.murmur3_128();

    public CachingHdfsInputFile(HdfsEnvironment environment, HdfsContext context, HdfsInputFile inputFile,
                                CacheManager cacheManager, AlluxioConfiguration alluxioConf)
    {
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
        this.inputFile = inputFile;
        this.file = hadoopPath(inputFile.location());
        this.cacheManager = cacheManager;
        this.alluxioConf = alluxioConf;
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        FileSystem fileSystem = environment.getFileSystem(context, file);
        FSDataInputStream input = environment.doAs(context.getIdentity(), () -> {
            FileInfo info = new FileInfo()
                    .setLastModificationTimeMs(inputFile.lastModified().toEpochMilli())
                    .setPath(file.toString())
                    .setFolder(false)
                    .setLength(inputFile.length());
            String cacheIdentifier = hashFunction.hashString(file.toString() + inputFile.lastModified().toEpochMilli(), UTF_8).toString();
            URIStatus uriStatus = new URIStatus(info, CacheContext.defaults().setCacheIdentifier(cacheIdentifier));
            return new FSDataInputStream(new HdfsFileInputStream(
                    new LocalCacheFileInStream(uriStatus, (uri) -> new AlluxioHdfsInputStream(fileSystem.open(file)), cacheManager, alluxioConf),
                    statistics));
        });
        return new HdfsInput(input, this);
    }

    @Override
    public TrinoInputStream newStream() throws IOException
    {
        return inputFile.newStream();
    }

    @Override
    public long length() throws IOException
    {
        return inputFile.length();
    }

    @Override
    public Instant lastModified() throws IOException
    {
        return inputFile.lastModified();
    }

    @Override
    public boolean exists() throws IOException
    {
        return inputFile.exists();
    }

    @Override
    public Location location()
    {
        return inputFile.location();
    }
}
