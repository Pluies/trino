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
import io.trino.client.NodeVersion;
import io.trino.filesystem.cache.NodeProvider;
import io.trino.metadata.InternalNode;
import io.trino.spi.Node;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Math.abs;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConsistentHashingNodeProvider
{
    @Test
    public void testConsistentHashing()
    {
        TestingNodeManager nodeManager = new TestingNodeManager(true);
        nodeManager.addNode(node("test-1"));
        nodeManager.addNode(node("test-2"));
        nodeManager.addNode(node("test-3"));
        ConsistentHashingNodeProvider nodeProvider = new ConsistentHashingNodeProvider(nodeManager);
        assertFairDistribution(nodeProvider, nodeManager.getWorkerNodes());
        nodeManager.removeNode(node("test-2"));
        assertFairDistribution(nodeProvider, nodeManager.getWorkerNodes());
        nodeManager.addNode(node("test-4"));
        nodeManager.addNode(node("test-5"));
        assertFairDistribution(nodeProvider, nodeManager.getWorkerNodes());
    }

    @Test
    public void testConsistentHashingFairRedistribution()
    {
        TestingNodeManager nodeManager = new TestingNodeManager(true);
        nodeManager.addNode(node("test-1"));
        nodeManager.addNode(node("test-2"));
        nodeManager.addNode(node("test-3"));
        ConsistentHashingNodeProvider nodeProvider = new ConsistentHashingNodeProvider(nodeManager);
        Map<String, Set<Integer>> distribution = getDistribution(nodeProvider);
        nodeManager.removeNode(node("test-1"));
        Map<String, Set<Integer>> removeOne = getDistribution(nodeProvider);
        assertMinimalRedistribution(distribution, removeOne);
        nodeManager.addNode(node("test-1"));
        Map<String, Set<Integer>> addOne = getDistribution(nodeProvider);
        assertMinimalRedistribution(removeOne, addOne);
        assertThat(addOne).isEqualTo(distribution);
        nodeManager.addNode(node("test-4"));
        Map<String, Set<Integer>> addTwo = getDistribution(nodeProvider);
        assertMinimalRedistribution(addOne, addTwo);
    }

    private static void assertFairDistribution(NodeProvider nodeProvider, Set<Node> nodeNames)
    {
        int n = 1000;
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < n; i++) {
            counts.merge(nodeProvider.getHosts(String.valueOf(i)).get(0).getHostText(), 1, Math::addExact);
        }
        assertThat(nodeNames.stream().map(m -> m.getHostAndPort().getHostText()).collect(Collectors.toSet())).isEqualTo(counts.keySet());
        counts.values().forEach(c -> assertThat(abs(c - n / nodeNames.size()) < 0.1 * n).isTrue());
    }

    private void assertMinimalRedistribution(Map<String, Set<Integer>> oldDistribution, Map<String, Set<Integer>> newDistribution)
    {
        oldDistribution.entrySet().stream().filter(e -> newDistribution.containsKey(e.getKey())).forEach(entry -> {
            int sameKeySize = Sets.intersection(newDistribution.get(entry.getKey()), entry.getValue()).size();
            int oldKeySize = entry.getValue().size();
            assertThat(abs(sameKeySize - oldKeySize) < oldKeySize / oldDistribution.size()).isTrue();
        });
    }

    private Map<String, Set<Integer>> getDistribution(ConsistentHashingNodeProvider nodeProvider)
    {
        int n = 1000;
        Map<String, Set<Integer>> distribution = new HashMap<>();
        for (int i = 0; i < n; i++) {
            String host = nodeProvider.getHosts(String.valueOf(i)).get(0).getHostText();
            distribution.computeIfAbsent(host, (k) -> new HashSet<>()).add(i);
        }
        return distribution;
    }

    private static Node node(String nodeName)
    {
        return new InternalNode(nodeName, URI.create("http://" + nodeName + "/"), NodeVersion.UNKNOWN, false);
    }
}
