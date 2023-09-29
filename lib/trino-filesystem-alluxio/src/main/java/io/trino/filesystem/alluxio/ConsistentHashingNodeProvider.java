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

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.trino.filesystem.cache.NodeProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ConsistentHashingNodeProvider
        implements NodeProvider
{
    private final NodeManager nodeManager;

    private final ConsistentHash<TrinoNode> consistentHashRing = HashRing.<TrinoNode>newBuilder()
            .hasher(DefaultHasher.METRO_HASH)
            .build();

    @Inject
    public ConsistentHashingNodeProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public List<HostAddress> getHosts(String identifier)
    {
        refreshHashRing();
        return consistentHashRing.locate(identifier)
                .stream()
                .map(n -> n.node().getHostAndPort())
                .collect(Collectors.toList());
    }

    private void refreshHashRing()
    {
        // Avoid synchronized block if possible
        // TODO: It would be better if the NodeManager exposed a "node generation number" to avoid comparing the sets
        if (!consistentHashRing.getNodes().equals(nodeManager.getWorkerNodes().stream().map(TrinoNode::new).collect(Collectors.toSet()))) {
            synchronizedRefreshHashRing();
        }
    }

    private synchronized void synchronizedRefreshHashRing()
    {
        Set<TrinoNode> nodes = nodeManager.getWorkerNodes().stream().map(TrinoNode::new).collect(Collectors.toSet());
        Set<TrinoNode> hashRingNodes = consistentHashRing.getNodes();
        Set<TrinoNode> removedNodes = Sets.difference(hashRingNodes, nodes);
        Set<TrinoNode> newNodes = Sets.difference(nodes, hashRingNodes);
        // Avoid acquiring a write lock in consistentHashRing if possible
        if (!newNodes.isEmpty()) {
            consistentHashRing.addAll(newNodes);
        }
        if (!removedNodes.isEmpty()) {
            removedNodes.forEach(consistentHashRing::remove);
        }
    }

    private record TrinoNode(Node node)
            implements org.ishugaliy.allgood.consistent.hash.node.Node
    {
        @Override
        public String getKey()
        {
            return node.getNodeIdentifier();
        }
    }
}
