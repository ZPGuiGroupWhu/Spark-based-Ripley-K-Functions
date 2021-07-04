package org.wysrc.spatialK.index.kdbtree;

import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.util.Assert;
import org.wysrc.spatialK.geom.SpatiotemporalEnvelope;
import org.wysrc.spatialK.geom.SpatiotemporalGeometry;
import org.wysrc.spatialK.geom.SpatiotemporalPoint;
import org.wysrc.spatialK.index.SpatiotemporalIndex;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class KDBTree implements SpatiotemporalIndex, Serializable {
    private final int maxChildrenPerNode;
    private final int maxItemsPerNode;
    private final double underflowFraction;
    private final SpatiotemporalEnvelope envelope;
    private AtomicInteger idBase = new AtomicInteger(0);
    private KDBNode root;
    private CyclicSplitter cyclicSplitter;

    public KDBTree(SpatiotemporalEnvelope envelope) {
        this(10, 10, envelope);
    }

    public KDBTree(int maxChildrenPerNode, int maxItemsPerNode, SpatiotemporalEnvelope envelope) {
        this(maxChildrenPerNode, maxItemsPerNode, 0.2, envelope, 0, 1, 2);
    }

    public KDBTree(int maxChildrenPerNode, int maxItemsPerNode, double underflowFraction, SpatiotemporalEnvelope envelope, int XIndex, int YIndex, int TimeIndex) {
        Assert.isTrue(maxChildrenPerNode > 0 && maxItemsPerNode > 0, "maxChildrenPerNode and maxItemsPerNode must be greater than 0!");
        Assert.isTrue(envelope!=null && !envelope.isNull(), "Spatiotemporal envelope of KDBTree cannot be null.");
        this.maxChildrenPerNode = maxChildrenPerNode;
        this.maxItemsPerNode = maxItemsPerNode;
        this.underflowFraction = underflowFraction;
        this.envelope = envelope;
        this.root = new KDBNode(null, envelope);
        this.cyclicSplitter = new CyclicSplitter(XIndex, YIndex, TimeIndex);
    }

    public int getMaxChildrenPerNode() {
        return maxChildrenPerNode;
    }

    public int getMaxItemsPerNode() {
        return maxItemsPerNode;
    }

    public double getUnderflowFraction() {
        return underflowFraction;
    }

    public SpatiotemporalEnvelope getEnvelope() {
        return envelope;
    }

    public int getIdBase() {
        return this.idBase.get();
    }

    public void setIdBase(int idBase) {
        this.idBase.set(idBase);
    }

    public KDBNode getRoot() {
        return root;
    }

    public CyclicSplitter getCyclicSplitter() {
        return cyclicSplitter;
    }

    public boolean isEmpty() {
        if(root.isLeaf()) {
            return root.getItems().isEmpty();
        }
        return false;
    }

    @Override
    public void insert(SpatiotemporalEnvelope itemEnvelope, Object item) {
//        Assert.isTrue(item instanceof SpatiotemporalGeometry, "Cannot insert item except SpatiotemporalGeometry.");
        insert(root, itemEnvelope, item);
    }

    private boolean insert(KDBNode insertNode, SpatiotemporalEnvelope itemEnvelope, Object item) {
        if(itemEnvelope.intersects(insertNode.getEnvelope())) {
            if(!insertNode.isLeaf()) {
                for(KDBNode child: insertNode.getChildren()) {
                    if(insert(child, itemEnvelope, item)) {
                        if(insertNode.getChildren().size() > maxChildrenPerNode) {
                            splitNode(insertNode);
                        }
                        // avoid insert duplicate items that lie on the boundary
                        return true;
                    }
                }
            } else {
                insertNode.addItem(new ItemEnveloped(itemEnvelope, item));
                if(insertNode.getItems().size() > maxItemsPerNode) {
                    splitNode(insertNode);
                }
                return true;
            }
        }
        return false;
    }

    private void splitNode(KDBNode node) {
        // split for child nodes
        KDBNode[] childNodes = splitNodeToChildren(node);

        // if the split failed, stop the split this time.
        if(childNodes == null) {
            return;
        }

        if(node == root) {
            if(!node.isLeaf()) {
                node.getChildren().clear();
            } else {
                node.getItems().clear();
            }

            // record the split order
            node.setSplitOrder(cyclicSplitter.getCurrentOrder());
            node.setSplittingPlane(cyclicSplitter);

            // sets the relations between the parent and new nodes
            node.addChild(childNodes[0]);
            node.addChild(childNodes[1]);
            childNodes[0].setParent(node);
            childNodes[1].setParent(node);
        } else {
            KDBNode parent = node.getParent();
            // remove relation between current node and its parent
            parent.removeChild(node);
            node.setParent(null);

            // record the split order
            parent.setSplitOrder(cyclicSplitter.getCurrentOrder());
            parent.setSplittingPlane(cyclicSplitter);

            // sets the relations between the parent and new nodes
            parent.addChild(childNodes[0]);
            parent.addChild(childNodes[1]);
            childNodes[0].setParent(parent);
            childNodes[1].setParent(parent);
        }

        // check if the leaf nodes are underflow
//        if(childNodes[0].isLeaf() && childNodes[0].getItems().size() < underflowFraction*maxItemsPerNode) {
//            reorganize(childNodes[0]);
//        } else if(childNodes[1].isLeaf() && childNodes[1].getItems().size() < underflowFraction*maxItemsPerNode) {
//            reorganize(childNodes[1]);
//        }
    }

    private KDBNode[] splitNodeToChildren(KDBNode node) {
        return splitNodeToChildren(node, false);
    }

    private KDBNode[] splitNodeToChildren(KDBNode node, boolean useParentSplitter) {
        if(! useParentSplitter) {
            // if the split is not propagated from parent, get new splitter
            boolean success = cyclicSplitter.getSplitter(node);
            int count = 1;
            while(node.isLeaf() && !success && count < 3) {
                // try X, Y, Time dimension splitter
                success = cyclicSplitter.getSplitter(node);
                count++;
            }
            if(!success) {
                // if fail to get valid splitter, stop the split
                return null;
            }
        }

        // create new child nodes
        SpatiotemporalEnvelope[] childEnvelopes = cyclicSplitter.splitEnvelope(node.getEnvelope());
        KDBNode lowerChild = new KDBNode(null, childEnvelopes[0]);
        lowerChild.setSplitOrder(cyclicSplitter.getCurrentOrder());
        lowerChild.setSplittingPlane(cyclicSplitter);
        KDBNode higherChild = new KDBNode(null, childEnvelopes[1]);
        higherChild.setSplitOrder(cyclicSplitter.getCurrentOrder());
        higherChild.setSplittingPlane(cyclicSplitter);

        if(!node.isLeaf()) {
            // if the node is non-leaf node, split its children according to splitted envelopes
            Iterator childrenIterator = node.getChildren().iterator();
            while(childrenIterator.hasNext()) {
                KDBNode child = (KDBNode) childrenIterator.next();
                if(childEnvelopes[0].covers(child.getEnvelope())) {
                    lowerChild.addChild(child);
                    child.setParent(lowerChild);
                } else if(childEnvelopes[1].covers(child.getEnvelope())) {
                    higherChild.addChild(child);
                    child.setParent(higherChild);
                } else {
                    KDBNode[] childNodes = splitNodeToChildren(child, true);
                    child.setParent(null);
                    lowerChild.addChild(childNodes[0]);
                    childNodes[0].setParent(lowerChild);
                    higherChild.addChild(childNodes[1]);
                    childNodes[1].setParent(higherChild);
                }
                childrenIterator.remove();
            }
        } else {
            // if the node is leaf node, split its items
            List<ItemEnveloped>[] itemLists = cyclicSplitter.splitItems(node.getItems());

            // sets the items to new child nodes
            lowerChild.setItems(itemLists[0]);
            higherChild.setItems(itemLists[1]);
        }

        return new KDBNode[]{lowerChild, higherChild};
    }

    @Override
    public List query(SpatiotemporalEnvelope queryEnvelope) {
        List queryResult = new ArrayList();
        query(root, queryEnvelope, queryResult);
        return queryResult;
    }

    private void query(KDBNode queryNode, SpatiotemporalEnvelope queryEnvelope, List queryResult) {
        if(queryEnvelope.intersects(queryNode.getEnvelope())) {
            if(!queryNode.isLeaf()) {
                for(KDBNode child: queryNode.getChildren()) {
                    query(child, queryEnvelope, queryResult);
                }
            } else {
                for(ItemEnveloped item : queryNode.getItems()) {
                    if(queryEnvelope.intersects((SpatiotemporalEnvelope) item.getEnvelope())) {
                        queryResult.add(item.getItem());
                    }
                }
            }
        }
    }

    public List<SpatiotemporalEnvelope> queryLeafEnvelope(SpatiotemporalGeometry geometry) {
        List<SpatiotemporalEnvelope> queryList = new ArrayList<>();
        SpatiotemporalEnvelope envelope = geometry.getEnvelopeInternal();
        queryLeafEnvelope(root, envelope, queryList);

        if(geometry instanceof SpatiotemporalPoint && queryList.size() > 1) {
            return queryList.subList(0, 1);
        }
        return queryList;
    }

    private void queryLeafEnvelope(KDBNode queryNode, SpatiotemporalEnvelope queryEnvelope, List<SpatiotemporalEnvelope> queryList) {
        if(queryEnvelope.intersects(queryNode.getEnvelope())) {
            if(!queryNode.isLeaf()) {
                for(KDBNode child: queryNode.getChildren()) {
                    queryLeafEnvelope(child, queryEnvelope, queryList);
                }
            } else {
                queryList.add(queryNode.getEnvelope());
            }
        }
    }

    @Override
    public void query(SpatiotemporalEnvelope queryEnvelope, ItemVisitor visitor) {
        query(root, queryEnvelope, visitor);
    }

    private void query(KDBNode queryNode, SpatiotemporalEnvelope queryEnvelope, ItemVisitor visitor) {
        if(queryEnvelope.intersects(queryNode.getEnvelope())) {
            if(!queryNode.isLeaf()) {
                for(KDBNode child: queryNode.getChildren()) {
                    query(child, queryEnvelope, visitor);
                }
            } else {
                for(ItemEnveloped item: queryNode.getItems()) {
                    if(queryEnvelope.intersects((SpatiotemporalEnvelope) item.getEnvelope())) {
                        visitor.visitItem(item);
                    }
                }
            }
        }
    }

    @Override
    public boolean remove(SpatiotemporalEnvelope itemEnvelope, Object item) {
//        Assert.isTrue(item instanceof SpatiotemporalGeometry, "Cannot remove item except SpatiotemporalGeometry.");
        return remove(root, itemEnvelope, item);
    }

    private boolean remove(KDBNode node, SpatiotemporalEnvelope itemEnvelope, Object item) {
        if(itemEnvelope.intersects(node.getEnvelope())) {
            if(!node.isLeaf()) {
                for(KDBNode child: node.getChildren()) {
                    if(remove(child, itemEnvelope, item)) {
                        return true;
                    }
                }
            } else {
                boolean isRemoved = node.removeItem(new ItemEnveloped(itemEnvelope, item));
                // check if the data node is underflow
                if(isRemoved && node.getItems().size() < underflowFraction*maxItemsPerNode) {
                    reorganize(node);
                }
                return isRemoved;
            }
        }
        return false;
    }

    private void reorganize(KDBNode node) {
        // find joinable DataNodes from parent
        KDBNode joinableNode = findJoinableNode(node);
        if(joinableNode != null) {
            // merge the two leaf nodes
            mergeNodes(node, joinableNode);
        } else {
            // if the parent could be split to satisfy the children and items count limit,
            // merge all the children the this leaf node's parent, and split it recursively
            tryResplitNode(node.getParent());
        }
    }

    private KDBNode findJoinableNode(KDBNode node) {
        Assert.isTrue(node != null);
        for(KDBNode child: node.getParent().getChildren()) {
            if(!node.equals(child)) {
                SpatiotemporalEnvelope otherEnvelope = child.getEnvelope();
                if(node.getItems().size() + child.getItems().size() < maxItemsPerNode &&
                        node.getEnvelope().isAdjacentTo(otherEnvelope)) {
                    return child;
                }
            }
        }
        return null;
    }

    private void mergeNodes(KDBNode node1, KDBNode node2) {
        Assert.isTrue(node1 != null && node2 != null);
        Assert.isTrue(node1.getParent() == node2.getParent());
        // get envelope after merge
        SpatiotemporalEnvelope mergedEnvelope = new SpatiotemporalEnvelope(node1.getEnvelope());
        mergedEnvelope.expandToInclude(node2.getEnvelope());
        // get items after merge
        List<ItemEnveloped> mergedItems = new ArrayList<>();
        mergedItems.addAll(node1.getItems());
        mergedItems.addAll(node2.getItems());
        // create new merged nodes
        KDBNode mergedNode = new KDBNode(node1.getParent(), mergedEnvelope);
        node1.getParent().addChild(mergedNode);
        mergedNode.getParent().removeChild(node1);
        node1.setParent(null);
        mergedNode.getParent().removeChild(node2);
        node2.setParent(null);
    }

    private void tryResplitNode(KDBNode node) {
        Assert.isTrue(node != null);
        List<ItemEnveloped> totalItems = new ArrayList<>();
        for(KDBNode child: node.getChildren()) {
            totalItems.addAll(child.getItems());
        }
        if(totalItems.size()/Integer.highestOneBit(maxChildrenPerNode) + 1 < maxItemsPerNode) {
            for(KDBNode child: node.getChildren()) {
                child.setParent(null);
            }
            node.getChildren().clear();
            node.setItems(totalItems);
            List<KDBNode> nodesAfterSplit = new ArrayList<>();
            splitNodesRecursively(Arrays.asList(node), nodesAfterSplit, Integer.highestOneBit(maxChildrenPerNode) - 1);
            for(KDBNode nodeAfterSplit: nodesAfterSplit) {
                node.addChild(nodeAfterSplit);
                nodeAfterSplit.setParent(node);
            }
            node.getItems().clear();
        }
    }

    private void splitNodesRecursively(List<KDBNode> nodesToSplit, List<KDBNode> nodesAfterSplit, int maxSplitTimes) {
        for(KDBNode node: nodesToSplit) {
            int count = 0;
            boolean success;
            do {
                // try X, Y, Time dimension splitter
                success = cyclicSplitter.getSplitter(node);
                count++;
            } while(!success && count < 3);
            if(!success) {
                // if fail to get valid splitter, stop split for the current node, go to the next
                nodesAfterSplit.add(node);
                continue;
            }
            SpatiotemporalEnvelope[] childEnvelopes = cyclicSplitter.splitEnvelope(node.getEnvelope());
            List<ItemEnveloped>[] itemLists = cyclicSplitter.splitItems(node.getItems());
            KDBNode lowerNode = new KDBNode(null, childEnvelopes[0]);
            lowerNode.setSplitOrder(cyclicSplitter.getCurrentOrder());
            lowerNode.setSplittingPlane(cyclicSplitter);
            lowerNode.setItems(itemLists[0]);
            KDBNode higherNode = new KDBNode(null, childEnvelopes[1]);
            higherNode.setSplitOrder(cyclicSplitter.getCurrentOrder());
            higherNode.setSplittingPlane(cyclicSplitter);
            higherNode.setItems(itemLists[1]);
            if(lowerNode.getItems().size() > maxItemsPerNode && higherNode.getItems().size() > maxItemsPerNode && maxSplitTimes > 0) {
                splitNodesRecursively(Arrays.asList(lowerNode, higherNode), nodesAfterSplit, maxSplitTimes - 1);
            } else if(lowerNode.getItems().size() > maxItemsPerNode && maxSplitTimes > 0) {
                nodesAfterSplit.add(higherNode);
                splitNodesRecursively(Arrays.asList(lowerNode), nodesAfterSplit, maxSplitTimes - 1);
            } else if(higherNode.getItems().size() > maxItemsPerNode && maxSplitTimes > 0) {
                nodesAfterSplit.add(lowerNode);
                splitNodesRecursively(Arrays.asList(higherNode), nodesAfterSplit, maxSplitTimes - 1);
            } else {
                nodesAfterSplit.add(lowerNode);
                nodesAfterSplit.add(higherNode);
            }
        }
    }

    public void assignLeafEnvelopeIds() {
//        idBase.set(0);
        assignLeafEnvelopeIds(root);
    }

    private void assignLeafEnvelopeIds(KDBNode node) {
        if(!node.isLeaf()) {
            for(KDBNode child: node.getChildren()) {
                assignLeafEnvelopeIds(child);
            }
        } else {
            node.getEnvelope().id = idBase.getAndIncrement();
        }
    }

    public List<SpatiotemporalEnvelope> getLeafEnvelopes(boolean dropItems) {
        List<SpatiotemporalEnvelope> leafEnvelopes = new ArrayList<>();
        getLeafEnvelopes(root, leafEnvelopes, dropItems);
        return leafEnvelopes;
    }

    private void getLeafEnvelopes(KDBNode node, List<SpatiotemporalEnvelope> leafEnvelopes, boolean dropItems) {
        if(!node.isLeaf()) {
            for(KDBNode child: node.getChildren()) {
                getLeafEnvelopes(child, leafEnvelopes, dropItems);
            }
        } else {
            leafEnvelopes.add(node.getEnvelope());
            if(dropItems) {
                node.getItems().clear();
            }
        }
    }

    public int getNumItems() {
        return getNumItems(root);
    }

    private int getNumItems(KDBNode node) {
        int numItems = 0;
        if(!node.isLeaf()) {
            for(KDBNode child: node.getChildren()) {
                numItems += getNumItems(child);
            }
        } else {
            numItems += node.getItems().size();
        }
        return numItems;
    }
}
