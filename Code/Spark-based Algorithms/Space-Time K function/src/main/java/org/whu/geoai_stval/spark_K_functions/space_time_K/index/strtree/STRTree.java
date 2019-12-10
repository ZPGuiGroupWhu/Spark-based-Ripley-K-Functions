package org.whu.geoai_stval.spark_K_functions.space_time_K.index.strtree;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.ItemVisitor;
import com.vividsolutions.jts.index.strtree.Boundable;
import com.vividsolutions.jts.index.strtree.ItemBoundable;
import com.vividsolutions.jts.util.Assert;
import org.whu.geoai_stval.spark_K_functions.space_time_K.geom.SpatiotemporalEnvelope;
import org.whu.geoai_stval.spark_K_functions.space_time_K.geom.SpatiotemporalGeometry;
import org.whu.geoai_stval.spark_K_functions.space_time_K.geom.SpatiotemporalPoint;
import org.whu.geoai_stval.spark_K_functions.space_time_K.index.SpatiotemporalIndex;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class STRTree implements SpatiotemporalIndex, Serializable {
    private STRNode root;
    private boolean built = false;
    private List itemsBoundables = new ArrayList();
    private SpatiotemporalEnvelope bounds;
    private int nodeCapacity;
    private static final int DEFAULT_NODE_CAPACITY = 10;
    private int compareTimes = 0;

    private static final class XComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            Envelope e1 = ((SpatiotemporalEnvelope) ((Boundable) o1).getBounds()).spatialEnvelope;
            Envelope e2 = ((SpatiotemporalEnvelope) ((Boundable) o2).getBounds()).spatialEnvelope;
            return (int) Math.signum(e1.getMinX() - e2.getMinX());
        }
    }

    private static final class YComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            Envelope e1 = ((SpatiotemporalEnvelope) ((Boundable) o1).getBounds()).spatialEnvelope;
            Envelope e2 = ((SpatiotemporalEnvelope) ((Boundable) o2).getBounds()).spatialEnvelope;
            return (int) Math.signum(e1.getMinY() - e2.getMinY());
        }
    }

    private static final class TimeComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            LocalDateTime t1 = ((SpatiotemporalEnvelope) ((Boundable) o1).getBounds()).startTime;
            LocalDateTime t2 = ((SpatiotemporalEnvelope) ((Boundable) o2).getBounds()).startTime;
            return t1.compareTo(t2);
        }
    }

    /**
     * Default Constructor
     */
    public STRTree() {
        this(DEFAULT_NODE_CAPACITY, null);
    }

    /**
     * Constructs a STRTree with specified node capacity
     *
     * @param nodeCapacity the maximum number of child nodes in a node
     */
    public STRTree(int nodeCapacity, SpatiotemporalEnvelope bounds) {
        Assert.isTrue(nodeCapacity > 1, "Node capacity of each node must be greater than 1.");
        this.nodeCapacity = nodeCapacity;
        this.bounds = bounds;
    }

    public synchronized void build() {
        if(built) {
            return;
        }
        // if inserted items are empty, build a empty STRTree, else build higher levels recursively.
        root = itemsBoundables.isEmpty()? createNode(0): createHigherLevels(itemsBoundables, -1);
        // after building, the items are no longer needed
        itemsBoundables = null;
        built = true;
        if(this.bounds == null) {
            this.bounds = root.getBounds();
        }
    }

    private STRNode createNode(int level) {
        return new STRNode(level);
    }

    private STRNode createNode(int level, SpatiotemporalEnvelope bounds) {
        return new STRNode(level, bounds);
    }

    private STRNode lastNode(List nodes) {
        return (STRNode) nodes.get(nodes.size() - 1);
    }

    /**
     * Creates the levels higher than the given level.
     *
     * @param boundablesOfALevel the level to build on
     * @param level the level of boundables, of -1 if the boundables are item boundables (below the level 0)
     * @return the root
     */
    private STRNode createHigherLevels(List boundablesOfALevel, int level) {
        Assert.isTrue(!boundablesOfALevel.isEmpty());
        List parentBoundables = createParentBoundables(boundablesOfALevel, level + 1);
        if(parentBoundables.size() == 1) {
            return (STRNode) parentBoundables.get(0);
        }
        return createHigherLevels(parentBoundables, level + 1);
    }

    private List createParentBoundables(List childBoundables, int newLevel) {
        Assert.isTrue(! childBoundables.isEmpty());
        if(newLevel == 0 && bounds != null && !bounds.isNull()) {
            return createParentBoundablesForPartition(childBoundables);
        }
        int minLeafCount = (int) Math.ceil(childBoundables.size() / (double) getNodeCapacity());
        List<Object> XSortedChildBoundables = new ArrayList(childBoundables);
        Comparator<Object> comparator = new XComparator();
        XSortedChildBoundables.sort(comparator);
        List[] XSlices = sliceBoundables(XSortedChildBoundables, (int) Math.ceil(Math.pow(minLeafCount, 1/3.0)));
        return createParentBoundablesFromXSlices(XSlices, newLevel);
    }

    private List[] sliceBoundables(List childBoundables, int sliceCount) {
        int sliceCapacity = (int) Math.ceil(childBoundables.size() / (double) sliceCount);
        List[] slices = new List[sliceCount];
        Iterator boundableIterator = childBoundables.iterator();
        for(int i=0; i<sliceCount; i++) {
            slices[i] = new ArrayList();
            int boundablesAddedToSlice = 0;
            while(boundableIterator.hasNext() && boundablesAddedToSlice < sliceCapacity) {
                Boundable childBoundable = (Boundable) boundableIterator.next();
                slices[i].add(childBoundable);
                boundablesAddedToSlice++;
            }
        }
        return slices;
    }

    private List createParentBoundablesFromXSlices(List[] XSlices, int newLevel) {
        Assert.isTrue(XSlices.length > 0);
        List parentBoundables = new ArrayList();
        for(List XSlice: XSlices) {
            List<Object> YSortedChildBoundables = new ArrayList(XSlice);
            Comparator<Object> comparator = new YComparator();
            YSortedChildBoundables.sort(comparator);
            List[] YSlices = sliceBoundables(YSortedChildBoundables, XSlices.length);
            parentBoundables.addAll(createParentBoundablesFromYSlices(YSlices, newLevel));
        }
        return parentBoundables;
    }

    private List createParentBoundablesFromYSlices(List[] YSlices, int newLevel) {
        Assert.isTrue(YSlices.length > 0);
        List parentBoundables = new ArrayList();
        for(List YSlice: YSlices) {
            parentBoundables.addAll(createParentBoundablesFromYSlice(YSlice, newLevel));
        }
        return parentBoundables;
    }

    private List createParentBoundablesFromYSlice(List YSlice, int newLevel) {
        Assert.isTrue(! YSlice.isEmpty());
        List parentBoundables = new ArrayList();
        parentBoundables.add(createNode(newLevel));
        List<Object> TimeSortedChildBoundables = new ArrayList(YSlice);
        Comparator<Object> comparator = new TimeComparator();
        TimeSortedChildBoundables.sort(comparator);
        for(Object o: TimeSortedChildBoundables) {
            Boundable childBoundable = (Boundable) o;
            if(lastNode(parentBoundables).size() == getNodeCapacity()) {
                //lastNode(parentBoundables).getBounds();
                parentBoundables.add(createNode(newLevel));
            }
            lastNode(parentBoundables).addChild(childBoundable);
        }
        return parentBoundables;
    }

    private List createParentBoundablesForPartition(List childBoundables) {
        int minLeafCount = (int) Math.ceil(childBoundables.size() / (double) getNodeCapacity());
        List<Object> XSortedChildBoundables = new ArrayList(childBoundables);
        Comparator<Object> comparator = new XComparator();
        XSortedChildBoundables.sort(comparator);
        List[] XSlices = sliceBoundables(XSortedChildBoundables, (int) Math.ceil(Math.pow(minLeafCount, 1/3.0)));
        return createParentBoundablesFromXSlicesForPartition(XSlices);
    }

    private List createParentBoundablesFromXSlicesForPartition(List[] XSlices) {
        Assert.isTrue(XSlices.length > 0);
        List parentBoundables = new ArrayList();
        for(int i=0; i<XSlices.length; i++) {
            List<Object> YSortedChildBoundables = new ArrayList(XSlices[i]);
            Comparator<Object> comparator = new YComparator();
            YSortedChildBoundables.sort(comparator);
            List[] YSlices = sliceBoundables(YSortedChildBoundables, XSlices.length);
            double minX, maxX;
            if(i == 0) {
                minX = bounds.spatialEnvelope.getMinX();
            } else {
                minX = ((SpatiotemporalEnvelope) ((Boundable) XSlices[i].get(0)).getBounds()).spatialEnvelope.getMinX();
            }
            if(i == XSlices.length-1) {
                maxX = bounds.spatialEnvelope.getMaxX();
            } else {
                maxX = ((SpatiotemporalEnvelope) ((Boundable) XSlices[i].get(XSlices[i].size()-1)).getBounds()).spatialEnvelope.getMaxX();
            }
            parentBoundables.addAll(createParentBoundablesFromYSlicesForPartition(YSlices, minX, maxX));
        }
        return parentBoundables;
    }

    private List createParentBoundablesFromYSlicesForPartition(List[] YSlices, double minX, double maxX) {
        Assert.isTrue(YSlices.length > 0);
        List parentBoundables = new ArrayList();
        for(int i=0; i<YSlices.length; i++) {
            List<Object> TimeSortedChildBoundables = new ArrayList(YSlices[i]);
            Comparator<Object> comparator = new TimeComparator();
            TimeSortedChildBoundables.sort(comparator);
            List[] TimeSlices = sliceBoundables(TimeSortedChildBoundables, YSlices.length);
            double minY, maxY;
            if(i == 0) {
                minY = bounds.spatialEnvelope.getMinY();
            } else {
                minY = ((SpatiotemporalEnvelope) ((Boundable) YSlices[i].get(0)).getBounds()).spatialEnvelope.getMinY();
            }
            if(i == YSlices.length-1) {
                maxY = bounds.spatialEnvelope.getMaxY();
            } else {
                maxY = ((SpatiotemporalEnvelope) ((Boundable) YSlices[i].get(YSlices[i].size()-1)).getBounds()).spatialEnvelope.getMaxY();
            }
            parentBoundables.addAll(createParentBoundablesFromTimeSlicesForPartition(TimeSlices, minX, maxX, minY, maxY));
        }
        return parentBoundables;
    }

    private List createParentBoundablesFromTimeSlicesForPartition(List[] TimeSlices, double minX, double maxX, double minY, double maxY) {
        Assert.isTrue(TimeSlices.length > 0);
        List parentBoundables = new ArrayList();
        for(int i=0; i<TimeSlices.length; i++) {
            LocalDateTime minTime, maxTime;
            if(i == 0) {
                minTime = bounds.startTime;
            } else {
                minTime = ((SpatiotemporalEnvelope) ((Boundable) TimeSlices[i].get(0)).getBounds()).startTime;
            }
            if(i == TimeSlices.length-1) {
                maxTime = bounds.endTime;
            } else {
                maxTime = ((SpatiotemporalEnvelope) ((Boundable) TimeSlices[i].get(TimeSlices[i].size()-1)).getBounds()).endTime;
            }
            parentBoundables.add(createNode(0, new SpatiotemporalEnvelope(new Envelope(minX, maxX, minY, maxY), minTime, maxTime)));
            for(Object o: TimeSlices[i]) {
                Boundable childBoundable = (Boundable) o;
                lastNode(parentBoundables).addChild(childBoundable);
            }
        }
        return parentBoundables;
    }

    public STRNode getRoot() {
        build();
        return root;
    }

    public void setRoot(STRNode root) {
        this.root = root;
    }

    public SpatiotemporalEnvelope getBounds() {
        return bounds;
    }

    public int getNodeCapacity() {
        return nodeCapacity;
    }

    public List getItemsBoundables() {
        return itemsBoundables;
    }

    public void setItemsBoundables(List itemsBoundables) {
        this.itemsBoundables = itemsBoundables;
    }

    public boolean isBuilt() {
        return built;
    }

    public void setBuilt(boolean built) {
        this.built = built;
    }

    public boolean isEmpty() {
        if(!built) {
            return itemsBoundables.isEmpty();
        }
        return root.isEmpty();
    }

    public int size() {
        if(isEmpty()) {
            return 0;
        }
        build();
        return size(root);
    }

    private int size(STRNode node) {
        int size = 0;
        for(Object o: node.getChildren()) {
            Boundable childBoundable = (Boundable) o;
            if(childBoundable instanceof STRNode) {
                size += size((STRNode) childBoundable);
            } else if(childBoundable instanceof ItemBoundable) {
                size += 1;
            }
        }
        return size;
    }

    public int depth() {
        if(isEmpty()) {
            return 0;
        }
        build();
        return depth(root);
    }

    private int depth(STRNode node) {
        int maxChildDepth = 0;
        for(Object o: node.getChildren()) {
            Boundable childBoundable = (Boundable) o;
            if(childBoundable instanceof STRNode) {
                int childDepth = depth((STRNode) childBoundable);
                if(childDepth > maxChildDepth) {
                    maxChildDepth = childDepth;
                }
            } else {
                break;
            }
        }
        return maxChildDepth + 1;
    }

    @Override
    public void insert(SpatiotemporalEnvelope itemEnvelope, Object item) {
        if(itemEnvelope.isNull()) {
            return;
        }
        Assert.isTrue(!built, "Cannot insert items into an STRTree after it has been built.");
        itemsBoundables.add(new ItemBoundable(itemEnvelope, item));
    }

    @Override
    public List query(SpatiotemporalEnvelope queryEnvelope) {
        build();
        List queryResult = new ArrayList();
        compareTimes = 0;
        if(isEmpty()) {
            return queryResult;
        }
        if(queryEnvelope.intersects(root.getBounds())) {
            compareTimes++;
            query(root, queryEnvelope, queryResult);
        }
//        System.out.println(compareTimes);
//        queryResult.clear();
//        queryResult.add(compareTimes);
        return queryResult;
    }

    private void query(STRNode node, SpatiotemporalEnvelope queryEnvelope, List queryResult) {
        for(Object o: node.getChildren()) {
            Boundable childBoundable = (Boundable) o;
            if(! queryEnvelope.intersects((SpatiotemporalEnvelope) childBoundable.getBounds())) {
                compareTimes++;
                continue;
            }
            compareTimes++;
            if(childBoundable instanceof STRNode) {
                query((STRNode) childBoundable, queryEnvelope, queryResult);
            } else if(childBoundable instanceof ItemBoundable) {
                queryResult.add(((ItemBoundable) childBoundable).getItem());
            }
        }
    }

    public List<SpatiotemporalEnvelope> queryLeafEnvelope(SpatiotemporalGeometry geometry) {
        List<SpatiotemporalEnvelope> queryList = new ArrayList<>();
        SpatiotemporalEnvelope envelope = geometry.getEnvelopeInternal();
        if(envelope.intersects(root.getBounds())) {
            queryLeafEnvelope(root, envelope, queryList);
            if(geometry instanceof SpatiotemporalPoint) {
                return queryList.subList(0, 1);
            }
        }
        return queryList;
    }

    private void queryLeafEnvelope(STRNode queryNode, SpatiotemporalEnvelope queryEnvelope, List queryList) {
        for(Object child: queryNode.getChildren()) {
            Boundable childBoundable = (Boundable) child;
            if(! queryEnvelope.intersects((SpatiotemporalEnvelope) childBoundable.getBounds())) {
                continue;
            }
            if(childBoundable instanceof STRNode) {
                queryLeafEnvelope((STRNode) childBoundable, queryEnvelope, queryList);
            } else if(childBoundable instanceof ItemBoundable) {
                queryList.add(childBoundable.getBounds());
            }
        }
    }

    @Override
    public void query(SpatiotemporalEnvelope queryEnvelope, ItemVisitor visitor) {
        build();
        if(isEmpty()) {
            return;
        }
        if(root.getBounds().intersects(queryEnvelope)) {
            query(root, queryEnvelope, visitor);
        }
    }

    private void query(STRNode node, SpatiotemporalEnvelope queryEnvelope, ItemVisitor visitor) {
        for(Object o: node.getChildren()) {
            Boundable childBoundable = (Boundable) o;
            if(! queryEnvelope.intersects((SpatiotemporalEnvelope) childBoundable.getBounds())) {
                continue;
            }
            if(childBoundable instanceof STRNode) {
                query((STRNode) childBoundable, queryEnvelope, visitor);
            } else if(childBoundable instanceof ItemBoundable) {
                visitor.visitItem(((ItemBoundable) childBoundable).getItem());
            }
        }
    }

    @Override
    public boolean remove(SpatiotemporalEnvelope itemEnvelope, Object item) {
        build();
        if(isEmpty()) {
            return false;
        }
        if(itemEnvelope.intersects(root.getBounds())) {
            return remove(root, itemEnvelope, item);
        }
        return false;
    }

    private boolean remove(STRNode node, SpatiotemporalEnvelope itemEnvelope, Object item) {
        // first try removing the item from the current node
        boolean found = removeItem(node, item);
        if(found) {
            return true;
        }

        // then try removing the item from the lower nodes
        STRNode childToPrune = null;
        for(Object o: node.getChildren()) {
            Boundable childBoundable = (Boundable) o;
            if(! itemEnvelope.intersects((SpatiotemporalEnvelope) childBoundable.getBounds())) {
                continue;
            }
            if(childBoundable instanceof STRNode) {
                found = remove((STRNode) childBoundable, itemEnvelope, item);
                if(found) {
                    childToPrune = (STRNode) childBoundable;
                    break;
                }
            }
        }

        // prune child if possible
        if(childToPrune != null) {
            if(childToPrune.isEmpty()) {
                node.getChildren().remove(childToPrune);
            }
        }
        return found;
    }

    private boolean removeItem(STRNode node, Object item) {
        Boundable childToRemove = null;
        for(Object o: node.getChildren()) {
            Boundable childBoundable = (Boundable) o;
            if(childBoundable instanceof ItemBoundable) {
                if(((ItemBoundable) childBoundable).getItem() == item) {
                    childToRemove = childBoundable;
                }
            }
        }
        if(childToRemove != null) {
            return node.getChildren().remove(childToRemove);
        }
        return false;
    }

    public List boundablesAtLevel(int level) {
        List boundables = new ArrayList();
        boundablesAtLevel(root, level, boundables);
        return boundables;
    }

    private void boundablesAtLevel(STRNode node, int level, List boundables) {
        Assert.isTrue(level > -2);
        if(node.getLevel() == level) {
            boundables.add(node);
            return;
        }
        for(Object o: node.getChildren()) {
            Boundable childBoundable = (Boundable) o;
            if(childBoundable instanceof STRNode) {
                boundablesAtLevel((STRNode) childBoundable, level, boundables);
            } else {
                Assert.isTrue(childBoundable instanceof ItemBoundable);
                if(level == -1) {
                    boundables.add(childBoundable);
                }
            }
        }
    }
}
