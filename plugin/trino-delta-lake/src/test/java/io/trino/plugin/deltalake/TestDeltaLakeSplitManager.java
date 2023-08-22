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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.DataSize;
import io.trino.client.NodeVersion;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.util.ConsistentHashingNodeProvider;
import io.trino.hdfs.util.NodeProvider;
import io.trino.metadata.InternalNode;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.hdfs.util.NoneNodeProvider.NONE_NODE_PROVIDER;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeSplitManager
{
    private static final String TABLE_PATH = "/path/to/a/table";
    private static final String FILE_PATH = "directory/file";
    private static final MetadataEntry metadataEntry = new MetadataEntry(
            "id",
            "name",
            "description",
            new MetadataEntry.Format("provider", ImmutableMap.of()),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"val\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}",
            ImmutableList.of(),
            ImmutableMap.of(),
            0);
    private static final DeltaLakeTableHandle tableHandle = new DeltaLakeTableHandle(
            "schema",
            "table",
            true,
            TABLE_PATH,
            metadataEntry,
            TupleDomain.all(),
            TupleDomain.all(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            0);

    @Test
    public void testInitialSplits()
            throws ExecutionException, InterruptedException
    {
        long fileSize = 20_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(fileSize));
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig()
                .setMaxInitialSplits(1000)
                .setMaxInitialSplitSize(DataSize.ofBytes(5_000));
        double minimumAssignedSplitWeight = deltaLakeConfig.getMinimumAssignedSplitWeight();

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltaLakeConfig, NONE_NODE_PROVIDER);
        List<DeltaLakeSplit> splits = getSplits(splitManager, deltaLakeConfig);

        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(0, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(5_000, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(10_000, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(15_000, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()));

        assertEquals(splits, expected);
    }

    @Test
    public void testNonInitialSplits()
            throws ExecutionException, InterruptedException
    {
        long fileSize = 50_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(fileSize));
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig()
                .setMaxInitialSplits(5)
                .setMaxInitialSplitSize(DataSize.ofBytes(5_000))
                .setMaxSplitSize(DataSize.ofBytes(20_000));
        double minimumAssignedSplitWeight = deltaLakeConfig.getMinimumAssignedSplitWeight();

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltaLakeConfig, NONE_NODE_PROVIDER);
        List<DeltaLakeSplit> splits = getSplits(splitManager, deltaLakeConfig);

        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(0, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(5_000, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(10_000, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(15_000, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(20_000, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(25_000, 20_000, fileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(45_000, 5_000, fileSize, minimumAssignedSplitWeight, Optional.empty()));

        assertEquals(splits, expected);
    }

    @Test
    public void testSplitsFromMultipleFiles()
            throws ExecutionException, InterruptedException
    {
        long firstFileSize = 1_000;
        long secondFileSize = 20_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(firstFileSize), addFileEntryOfSize(secondFileSize));
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig()
                .setMaxInitialSplits(3)
                .setMaxInitialSplitSize(DataSize.ofBytes(2_000))
                .setMaxSplitSize(DataSize.ofBytes(10_000));
        double minimumAssignedSplitWeight = deltaLakeConfig.getMinimumAssignedSplitWeight();

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltaLakeConfig, NONE_NODE_PROVIDER);

        List<DeltaLakeSplit> splits = getSplits(splitManager, deltaLakeConfig);
        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(0, 1_000, firstFileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(0, 2_000, secondFileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(2_000, 2_000, secondFileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(4_000, 10_000, secondFileSize, minimumAssignedSplitWeight, Optional.empty()),
                makeSplit(14_000, 6_000, secondFileSize, minimumAssignedSplitWeight, Optional.empty()));
        assertEquals(splits, expected);
    }

    @Test
    public void testSplitsWithConsistentHashingNodeProvider()
            throws ExecutionException, InterruptedException
    {
        long firstFileSize = 1_000;
        long secondFileSize = 20_000;
        String firstFilePath = FILE_PATH + "first";
        String secondFilePath = FILE_PATH + "second";
        List<AddFileEntry> addFileEntries = ImmutableList.of(
                addFileEntryOfPathSize(firstFilePath, firstFileSize),
                addFileEntryOfPathSize(secondFilePath, secondFileSize));
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig()
                .setMaxInitialSplits(3)
                .setMaxInitialSplitSize(DataSize.ofBytes(2_000))
                .setMaxSplitSize(DataSize.ofBytes(10_000));
        double minimumAssignedSplitWeight = deltaLakeConfig.getMinimumAssignedSplitWeight();

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltaLakeConfig, makeTestingNodeProvider(5));

        List<DeltaLakeSplit> splits = getSplits(splitManager, deltaLakeConfig);

        // The "expected" nodes depend on the consistent hashing algorithm, the file path, and the node list
        // Any change to these parameters could change the expected node
        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplitWithPath(firstFilePath, 0, 1_000, firstFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(4)),
                makeSplitWithPath(secondFilePath, 0, 2_000, secondFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(1)),
                makeSplitWithPath(secondFilePath, 2_000, 2_000, secondFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(1)),
                makeSplitWithPath(secondFilePath, 4_000, 10_000, secondFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(1)),
                makeSplitWithPath(secondFilePath, 14_000, 6_000, secondFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(1)));
        assertEquals(splits, expected);

        // Now do the same but with a node removed, we should get different results
        DeltaLakeSplitManager splitManager2 = setupSplitManager(addFileEntries, deltaLakeConfig, makeTestingNodeProvider(4));

        List<DeltaLakeSplit> splits2 = getSplits(splitManager2, deltaLakeConfig);

        List<DeltaLakeSplit> expected2 = ImmutableList.of(
                makeSplitWithPath(firstFilePath, 0, 1_000, firstFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(3)),
                makeSplitWithPath(secondFilePath, 0, 2_000, secondFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(1)),
                makeSplitWithPath(secondFilePath, 2_000, 2_000, secondFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(1)),
                makeSplitWithPath(secondFilePath, 4_000, 10_000, secondFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(1)),
                makeSplitWithPath(secondFilePath, 14_000, 6_000, secondFileSize, minimumAssignedSplitWeight, expectNodeHostAndPort(1)));

        assertEquals(splits2, expected2);
    }

    private Optional<HostAddress> expectNodeHostAndPort(int i)
    {
        return Optional.of(makeNode(i, false).getHostAndPort());
    }

    private DeltaLakeSplitManager setupSplitManager(List<AddFileEntry> addFileEntries, DeltaLakeConfig deltaLakeConfig, NodeProvider nodeProvider)
    {
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();

        return new DeltaLakeSplitManager(
                typeManager,
                new TransactionLogAccess(
                        typeManager,
                        new CheckpointSchemaManager(typeManager),
                        deltaLakeConfig,
                        new FileFormatDataSourceStats(),
                        new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS),
                        new ParquetReaderConfig())
                {
                    @Override
                    public List<AddFileEntry> getActiveFiles(TableSnapshot tableSnapshot, ConnectorSession session)
                    {
                        return addFileEntries;
                    }
                },
                MoreExecutors.newDirectExecutorService(),
                deltaLakeConfig,
                HDFS_FILE_SYSTEM_FACTORY,
                nodeProvider);
    }

    private AddFileEntry addFileEntryOfSize(long fileSize)
    {
        return addFileEntryOfPathSize(FILE_PATH, fileSize);
    }

    private AddFileEntry addFileEntryOfPathSize(String filePath, long fileSize)
    {
        return new AddFileEntry(filePath, ImmutableMap.of(), fileSize, 0, false, Optional.empty(), Optional.empty(), ImmutableMap.of());
    }

    private Node makeNode(int i, boolean coordinator)
    {
        return new InternalNode("node" + i, URI.create("http://node" + i + ":8080"), new NodeVersion("testVersion"), coordinator);
    }

    private NodeProvider makeTestingNodeProvider(int workerNodeCount)
    {
        List<Node> nodes = Stream.concat(
                Stream.of(makeNode(0, true)),
                IntStream.range(1, workerNodeCount).mapToObj(i -> makeNode(i, false))).toList();
        NodeManager nodeManager = new TestingNodeManager(nodes);
        return new ConsistentHashingNodeProvider(nodeManager);
    }

    private List<DeltaLakeSplit> getSplits(DeltaLakeSplitManager splitManager, DeltaLakeConfig deltaLakeConfig)
            throws ExecutionException, InterruptedException
    {
        ConnectorSplitSource splitSource = splitManager.getSplits(
                new HiveTransactionHandle(false),
                testingConnectorSessionWithConfig(deltaLakeConfig),
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        ImmutableList.Builder<DeltaLakeSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> nextBatch = splitSource.getNextBatch(10).get().getSplits();
            splits.addAll(
                    nextBatch.stream()
                            .map(split -> (DeltaLakeSplit) split)
                            .collect(Collectors.toList()));
        }
        return splits.build();
    }

    private DeltaLakeSplit makeSplit(long start, long splitSize, long fileSize, double minimumAssignedSplitWeight, Optional<HostAddress> address)
    {
        return makeSplitWithPath(FILE_PATH, start, splitSize, fileSize, minimumAssignedSplitWeight, address);
    }

    private DeltaLakeSplit makeSplitWithPath(String filePath, long start, long splitSize, long fileSize, double minimumAssignedSplitWeight, Optional<HostAddress> address)
    {
        SplitWeight splitWeight = SplitWeight.fromProportion(Math.min(Math.max((double) fileSize / splitSize, minimumAssignedSplitWeight), 1.0));
        return new DeltaLakeSplit(TABLE_PATH + "/" + filePath, start, splitSize, fileSize, Optional.empty(), 0, address, splitWeight, TupleDomain.all(), ImmutableMap.of());
    }

    private ConnectorSession testingConnectorSessionWithConfig(DeltaLakeConfig deltaLakeConfig)
    {
        DeltaLakeSessionProperties sessionProperties = new DeltaLakeSessionProperties(deltaLakeConfig, new ParquetReaderConfig(), new ParquetWriterConfig());
        return TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
    }
}
