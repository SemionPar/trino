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
package io.trino.geospatial;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static io.trino.geospatial.KdbTree.buildKdbTree;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKdbTree
{
    @Test
    public void testSerde()
    {
        Rectangle extent = new Rectangle(0, 0, 9, 4);
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (double x = 0; x < 10; x += 1) {
            for (double y = 0; y < 5; y += 1) {
                rectangles.add(new Rectangle(x, y, x + 0.1, y + 0.2));
            }
        }

        testSerializationRoundtrip(buildKdbTree(100, extent, rectangles.build()));
        testSerializationRoundtrip(buildKdbTree(20, extent, rectangles.build()));
        testSerializationRoundtrip(buildKdbTree(10, extent, rectangles.build()));
    }

    private void testSerializationRoundtrip(KdbTree tree)
    {
        KdbTree treeCopy = KdbTreeUtils.fromJson(KdbTreeUtils.toJson(tree));
        assertThat(treeCopy).isEqualTo(tree);
    }

    @Test
    public void testSinglePartition()
    {
        testSinglePartition(0, 0);
        testSinglePartition(1, 2);
    }

    private void testSinglePartition(double width, double height)
    {
        Rectangle extent = new Rectangle(0, 0, 9, 4);
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (double x = 0; x < 10; x += 1) {
            for (double y = 0; y < 5; y += 1) {
                rectangles.add(new Rectangle(x, y, x + width, y + height));
            }
        }

        KdbTree tree = buildKdbTree(100, extent, rectangles.build());

        assertThat(tree.getLeaves().size()).isEqualTo(1);

        Map.Entry<Integer, Rectangle> entry = Iterables.getOnlyElement(tree.getLeaves().entrySet());
        assertThat(entry.getKey().intValue()).isEqualTo(0);
        assertThat(entry.getValue()).isEqualTo(extent);
    }

    @Test
    public void testSplitVertically()
    {
        testSplitVertically(0, 0);
        testSplitVertically(1, 2);
    }

    private void testSplitVertically(double width, double height)
    {
        Rectangle extent = new Rectangle(0, 0, 9, 4);
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (int x = 0; x < 10; x++) {
            for (int y = 0; y < 5; y++) {
                rectangles.add(new Rectangle(x, y, x + width, y + height));
            }
        }

        KdbTree treeCopy = buildKdbTree(25, extent, rectangles.build());

        Map<Integer, Rectangle> leafNodes = treeCopy.getLeaves();
        assertThat(leafNodes.size()).isEqualTo(2);
        assertThat(leafNodes.keySet()).isEqualTo(ImmutableSet.of(0, 1));
        assertThat(leafNodes.get(0)).isEqualTo(new Rectangle(0, 0, 4.5, 4));
        assertThat(leafNodes.get(1)).isEqualTo(new Rectangle(4.5, 0, 9, 4));

        assertPartitions(treeCopy, new Rectangle(1, 1, 2, 2), ImmutableSet.of(0));
        assertPartitions(treeCopy, new Rectangle(1, 1, 5, 2), ImmutableSet.of(0, 1));
    }

    @Test
    public void testSplitHorizontally()
    {
        testSplitHorizontally(0, 0);
        testSplitHorizontally(1, 2);
    }

    private void testSplitHorizontally(double width, double height)
    {
        Rectangle extent = new Rectangle(0, 0, 4, 9);
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (int x = 0; x < 5; x++) {
            for (int y = 0; y < 10; y++) {
                rectangles.add(new Rectangle(x, y, x + width, y + height));
            }
        }

        KdbTree tree = buildKdbTree(25, extent, rectangles.build());

        Map<Integer, Rectangle> leafNodes = tree.getLeaves();
        assertThat(leafNodes.size()).isEqualTo(2);
        assertThat(leafNodes.keySet()).isEqualTo(ImmutableSet.of(0, 1));
        assertThat(leafNodes.get(0)).isEqualTo(new Rectangle(0, 0, 4, 4.5));
        assertThat(leafNodes.get(1)).isEqualTo(new Rectangle(0, 4.5, 4, 9));

        // points inside and outside partitions
        assertPartitions(tree, new Rectangle(1, 1, 1, 1), ImmutableSet.of(0));
        assertPartitions(tree, new Rectangle(1, 6, 1, 6), ImmutableSet.of(1));
        assertPartitions(tree, new Rectangle(5, 1, 5, 1), ImmutableSet.of());

        // point on the border separating two partitions
        assertPartitions(tree, new Rectangle(1, 4.5, 1, 4.5), ImmutableSet.of(0, 1));

        // rectangles
        assertPartitions(tree, new Rectangle(1, 1, 2, 2), ImmutableSet.of(0));
        assertPartitions(tree, new Rectangle(1, 6, 2, 7), ImmutableSet.of(1));
        assertPartitions(tree, new Rectangle(1, 1, 2, 5), ImmutableSet.of(0, 1));
        assertPartitions(tree, new Rectangle(5, 1, 6, 2), ImmutableSet.of());
    }

    private void assertPartitions(KdbTree kdbTree, Rectangle envelope, Set<Integer> partitions)
    {
        Map<Integer, Rectangle> matchingNodes = kdbTree.findIntersectingLeaves(envelope);
        assertThat(matchingNodes.size()).isEqualTo(partitions.size());
        assertThat(matchingNodes.keySet()).isEqualTo(partitions);
    }

    @Test
    public void testEvenDistribution()
    {
        testEvenDistribution(0, 0);
        testEvenDistribution(1, 2);
    }

    private void testEvenDistribution(double width, double height)
    {
        Rectangle extent = new Rectangle(0, 0, 9, 4);
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (int x = 0; x < 10; x++) {
            for (int y = 0; y < 5; y++) {
                rectangles.add(new Rectangle(x, y, x + width, y + height));
            }
        }

        KdbTree tree = buildKdbTree(10, extent, rectangles.build());

        Map<Integer, Rectangle> leafNodes = tree.getLeaves();
        assertThat(leafNodes.size()).isEqualTo(6);
        assertThat(leafNodes.keySet()).isEqualTo(ImmutableSet.of(0, 1, 2, 3, 4, 5));
        assertThat(leafNodes.get(0)).isEqualTo(new Rectangle(0, 0, 2.5, 2.5));
        assertThat(leafNodes.get(1)).isEqualTo(new Rectangle(0, 2.5, 2.5, 4));
        assertThat(leafNodes.get(2)).isEqualTo(new Rectangle(2.5, 0, 4.5, 4));
        assertThat(leafNodes.get(3)).isEqualTo(new Rectangle(4.5, 0, 7.5, 2.5));
        assertThat(leafNodes.get(4)).isEqualTo(new Rectangle(4.5, 2.5, 7.5, 4));
        assertThat(leafNodes.get(5)).isEqualTo(new Rectangle(7.5, 0, 9, 4));
    }

    @Test
    public void testSkewedDistribution()
    {
        testSkewedDistribution(0, 0);
        testSkewedDistribution(1, 2);
    }

    private void testSkewedDistribution(double width, double height)
    {
        Rectangle extent = new Rectangle(0, 0, 9, 4);
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (int x = 0; x < 10; x++) {
            for (int y = 0; y < 5; y++) {
                rectangles.add(new Rectangle(x, y, x + width, y + height));
            }
        }

        for (double x = 5; x < 6; x += 0.2) {
            for (double y = 1; y < 2; y += 0.5) {
                rectangles.add(new Rectangle(x, y, x + width, y + height));
            }
        }

        KdbTree tree = buildKdbTree(10, extent, rectangles.build());

        Map<Integer, Rectangle> leafNodes = tree.getLeaves();
        assertThat(leafNodes.size()).isEqualTo(9);
        assertThat(leafNodes.keySet()).isEqualTo(ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8));
        assertThat(leafNodes.get(0)).isEqualTo(new Rectangle(0, 0, 1.5, 2.5));
        assertThat(leafNodes.get(1)).isEqualTo(new Rectangle(1.5, 0, 3.5, 2.5));
        assertThat(leafNodes.get(2)).isEqualTo(new Rectangle(0, 2.5, 3.5, 4));
        assertThat(leafNodes.get(3)).isEqualTo(new Rectangle(3.5, 0, 5.1, 1.75));
        assertThat(leafNodes.get(4)).isEqualTo(new Rectangle(3.5, 1.75, 5.1, 4));
        assertThat(leafNodes.get(5)).isEqualTo(new Rectangle(5.1, 0, 5.9, 1.75));
        assertThat(leafNodes.get(6)).isEqualTo(new Rectangle(5.9, 0, 9, 1.75));
        assertThat(leafNodes.get(7)).isEqualTo(new Rectangle(5.1, 1.75, 7.5, 4));
        assertThat(leafNodes.get(8)).isEqualTo(new Rectangle(7.5, 1.75, 9, 4));
    }

    @Test
    public void testCantSplitVertically()
    {
        testCantSplitVertically(0, 0);
        testCantSplitVertically(1, 2);
    }

    private void testCantSplitVertically(double width, double height)
    {
        Rectangle extent = new Rectangle(0, 0, 9 + width, 4 + height);
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (int y = 0; y < 5; y++) {
            for (int i = 0; i < 10; i++) {
                rectangles.add(new Rectangle(0, y, width, y + height));
                rectangles.add(new Rectangle(9, y, 9 + width, y + height));
            }
        }

        KdbTree tree = buildKdbTree(10, extent, rectangles.build());

        Map<Integer, Rectangle> leafNodes = tree.getLeaves();
        assertThat(leafNodes.size()).isEqualTo(10);
        assertThat(leafNodes.keySet()).isEqualTo(ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        assertThat(leafNodes.get(0)).isEqualTo(new Rectangle(0, 0, 4.5, 0.5));
        assertThat(leafNodes.get(1)).isEqualTo(new Rectangle(0, 0.5, 4.5, 1.5));
        assertThat(leafNodes.get(2)).isEqualTo(new Rectangle(0, 1.5, 4.5, 2.5));
        assertThat(leafNodes.get(3)).isEqualTo(new Rectangle(0, 2.5, 4.5, 3.5));
        assertThat(leafNodes.get(4)).isEqualTo(new Rectangle(0, 3.5, 4.5, 4 + height));
        assertThat(leafNodes.get(5)).isEqualTo(new Rectangle(4.5, 0, 9 + width, 0.5));
        assertThat(leafNodes.get(6)).isEqualTo(new Rectangle(4.5, 0.5, 9 + width, 1.5));
        assertThat(leafNodes.get(7)).isEqualTo(new Rectangle(4.5, 1.5, 9 + width, 2.5));
        assertThat(leafNodes.get(8)).isEqualTo(new Rectangle(4.5, 2.5, 9 + width, 3.5));
        assertThat(leafNodes.get(9)).isEqualTo(new Rectangle(4.5, 3.5, 9 + width, 4 + height));
    }

    @Test
    public void testCantSplit()
    {
        testCantSplit(0, 0);
        testCantSplit(1, 2);
    }

    private void testCantSplit(double width, double height)
    {
        Rectangle extent = new Rectangle(0, 0, 9 + width, 4 + height);
        ImmutableList.Builder<Rectangle> rectangles = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 5; j++) {
                rectangles.add(new Rectangle(0, 0, width, height));
                rectangles.add(new Rectangle(9, 4, 9 + width, 4 + height));
            }
        }

        KdbTree tree = buildKdbTree(10, extent, rectangles.build());

        Map<Integer, Rectangle> leafNodes = tree.getLeaves();
        assertThat(leafNodes.size()).isEqualTo(2);
        assertThat(leafNodes.keySet()).isEqualTo(ImmutableSet.of(0, 1));
        assertThat(leafNodes.get(0)).isEqualTo(new Rectangle(0, 0, 4.5, 4 + height));
        assertThat(leafNodes.get(1)).isEqualTo(new Rectangle(4.5, 0, 9 + width, 4 + height));
    }
}
