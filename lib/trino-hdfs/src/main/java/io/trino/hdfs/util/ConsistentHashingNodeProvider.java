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
package io.trino.hdfs.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ConsistentHashingNodeProvider
        implements NodeProvider
{
    private final NodeManager nodeManager;
    // Use murmur3_128 as recommended by Guava for a fast hash that stays stable between runs
    private final HashFunction hashFunction = Hashing.murmur3_128();

    @Inject
    public ConsistentHashingNodeProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public Optional<HostAddress> getHost(String identifier)
    {
        List<HostAddress> currentNodes = nodeManager.getWorkerNodes().stream()
                .map(Node::getHostAndPort)
                .sorted(Comparator.comparing(HostAddress::getHostText))
                .collect(toImmutableList());
        if (currentNodes.isEmpty()) {
            return Optional.empty();
        }
        int index = Hashing.consistentHash(hashFunction.hashUnencodedChars(identifier), currentNodes.size());
        return Optional.of(currentNodes.get(index));
    }
}
