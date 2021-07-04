package org.wysrc.crossK.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.wysrc.crossK.geom.SpatiotemporalEnvelope;
import org.wysrc.crossK.geom.SpatiotemporalGeometry;
import org.wysrc.crossK.index.kdbtree.CyclicSplitter;
import org.wysrc.crossK.index.kdbtree.ItemEnveloped;
import org.wysrc.crossK.index.kdbtree.KDBNode;
import org.wysrc.crossK.index.kdbtree.KDBTree;
import org.wysrc.crossK.index.strtree.STRNode;
import org.wysrc.crossK.index.strtree.STRTree;
import org.wysrc.crossK.utils.TimeUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class SpatiotemporalIndexSerde extends Serializer {
    private static final Logger log = Logger.getLogger(SpatiotemporalIndexSerde.class);
    private SpatiotemporalGeometrySerde spatiotemporalGeometrySerde;
    private static final byte NON_LEAF_NODE = 0;
    private static final byte LEAF_NODE = 1;

    public SpatiotemporalIndexSerde() {
        super();
        this.spatiotemporalGeometrySerde = new SpatiotemporalGeometrySerde();
    }

    public SpatiotemporalIndexSerde(SpatiotemporalGeometrySerde spatiotemporalGeometrySerde) {
        super();
        this.spatiotemporalGeometrySerde = spatiotemporalGeometrySerde;
    }

    public SpatiotemporalGeometrySerde getSpatiotemporalGeometrySerde() {
        return this.spatiotemporalGeometrySerde;
    }

    private enum IndexType {
        STRTREE(0),
        KDBTREE(1);

        private final int id;

        IndexType(int id) {
            this.id = id;
        }

        public static IndexType fromId(int id) {
            for(IndexType indexType: IndexType.values()) {
                if(indexType.id == id) {
                    return indexType;
                }
            }
            return null;
        }
    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        if(object instanceof STRTree) {
            writeType(output, IndexType.STRTREE);
            STRTree strTree = (STRTree) object;
            output.writeInt(strTree.getNodeCapacity());
            writeSpatiotemporalEnvelope(kryo, output, strTree.getBounds());
            output.writeBoolean(!strTree.isEmpty());
            if(!strTree.isEmpty()) {
                output.writeBoolean(strTree.isBuilt());
                if(strTree.isBuilt()) {
                    writeSTRNode(kryo, output, strTree.getRoot());
                } else {
                    List itemBoundables = strTree.getItemsBoundables();
                    output.writeInt(itemBoundables.size());

                    for(Object o: itemBoundables) {
                        if(! (o instanceof ItemBoundable)) {
                            throw new UnsupportedOperationException("Type of items in the STRTree is not supported: "
                                    + o.getClass().getName());
                        }
                        ItemBoundable itemBoundable = (ItemBoundable) o;
                        writeItemBoundable(kryo, output, itemBoundable);
                    }
                }
            }
        } else if(object instanceof KDBTree) {
            writeType(output, IndexType.KDBTREE);
            KDBTree kdbTree = (KDBTree) object;
            output.writeInt(kdbTree.getMaxChildrenPerNode());
            output.writeInt(kdbTree.getMaxItemsPerNode());
            output.writeDouble(kdbTree.getUnderflowFraction());
            writeSpatiotemporalEnvelope(kryo, output, kdbTree.getEnvelope());
            output.writeInt(kdbTree.getIdBase());
            CyclicSplitter cyclicSplitter = kdbTree.getCyclicSplitter();
            output.writeInt(cyclicSplitter.getX());
            output.writeInt(cyclicSplitter.getY());
            output.writeInt(cyclicSplitter.getTime());
            output.writeInt(cyclicSplitter.getCurrentOrder());
            output.writeInt(cyclicSplitter.getOrderBase());
            output.writeBoolean(!kdbTree.isEmpty());
            if(!kdbTree.isEmpty()) {
                writeKDBTree(kryo, output, kdbTree, cyclicSplitter);
            }
        } else {
            throw new UnsupportedOperationException("Index type is not supported: " + object.getClass().getName());
        }
    }

    private void writeType(Output output, IndexType indexType) {
        output.writeByte((byte) indexType.id);
    }

    private void writeSpatiotemporalEnvelope(Kryo kryo, Output output, SpatiotemporalEnvelope spatiotemporalEnvelope) {
        output.writeBoolean(spatiotemporalEnvelope != null && !spatiotemporalEnvelope.isNull());
        if(spatiotemporalEnvelope != null && !spatiotemporalEnvelope.isNull()) {
            spatiotemporalGeometrySerde.write(kryo, output, spatiotemporalEnvelope);
        }
    }

    private void writeSTRNode(Kryo kryo, Output output, STRNode strNode) {
        output.writeInt(strNode.getLevel());
        List children = strNode.getChildren();
        output.writeInt(children.size());
        writeSpatiotemporalEnvelope(kryo, output, strNode.getBounds());

        if(children.size() > 0) {
            Object o = children.get(0);
            if(o instanceof STRNode) {
                output.writeByte(NON_LEAF_NODE);
                for(Object child: children) {
                    writeSTRNode(kryo, output, (STRNode) child);
                }
            } else if(o instanceof ItemBoundable) {
                output.writeByte(LEAF_NODE);
                for(Object child: children) {
                    writeItemBoundable(kryo, output, (ItemBoundable) child);
                }
            } else {
                throw new UnsupportedOperationException("Node Type of STRTree is not supported: " + o.getClass().getName());
            }
        }
    }

    private void writeItemBoundable(Kryo kryo, Output output, ItemBoundable itemBoundable) {
        output.writeBoolean(itemBoundable.getBounds() != null);
        if(itemBoundable.getBounds() != null) {
            spatiotemporalGeometrySerde.write(kryo, output, itemBoundable.getBounds());
        }
        output.writeBoolean(itemBoundable.getItem() != null);
        if(itemBoundable.getItem() != null) {
            spatiotemporalGeometrySerde.write(kryo, output, itemBoundable.getItem());
        }
    }

    private void writeKDBTree(Kryo kryo, Output output, KDBTree kdbTree, CyclicSplitter cyclicSplitter) {
        writeSplitOrderAndSplittingPlane(output, kdbTree.getRoot(), cyclicSplitter);
        writeKDBChildrenOrItems(kryo, output, kdbTree.getRoot(), cyclicSplitter);
    }

    private void writeSplitOrderAndSplittingPlane(Output output, KDBNode kdbNode, CyclicSplitter cyclicSplitter) {
        int splitOrder = kdbNode.getSplitOrder();
        output.writeInt(splitOrder);
        if (splitOrder == cyclicSplitter.getX()) {
            output.writeDouble(kdbNode.getSplittingPlaneX());
        } else if(splitOrder == cyclicSplitter.getY()) {
            output.writeDouble(kdbNode.getSplittingPlaneY());
        } else if(splitOrder == cyclicSplitter.getTime()) {
            long time = TimeUtils.getTimestampFromDateTime(kdbNode.getSplittingPlaneTime());
            output.writeLong(time);
        }
    }

    private void writeKDBChildrenOrItems(Kryo kryo, Output output, KDBNode kdbNode, CyclicSplitter cyclicSplitter) {
        if(!kdbNode.isLeaf()) {
            output.writeByte(NON_LEAF_NODE);
            output.writeInt(kdbNode.getChildren().size());
            for(KDBNode child: kdbNode.getChildren()) {
                writeKDBNode(kryo, output, child, cyclicSplitter);
            }
        } else {
            output.writeByte(LEAF_NODE);
            output.writeInt(kdbNode.getItems().size());
            for(ItemEnveloped item: kdbNode.getItems()) {
                writeItemEnveloped(kryo, output, item);
            }
        }
    }

    private void writeKDBNode(Kryo kryo, Output output, KDBNode kdbNode, CyclicSplitter cyclicSplitter) {
        writeSpatiotemporalEnvelope(kryo, output, kdbNode.getEnvelope());
        writeSplitOrderAndSplittingPlane(output, kdbNode, cyclicSplitter);
        writeKDBChildrenOrItems(kryo, output, kdbNode, cyclicSplitter);
    }

    private void writeItemEnveloped(Kryo kryo, Output output, ItemEnveloped itemEnveloped) {
        output.writeBoolean(itemEnveloped.getEnvelope() != null);
        if(itemEnveloped.getEnvelope() != null) {
            spatiotemporalGeometrySerde.write(kryo, output, itemEnveloped.getEnvelope());
        }
        output.writeBoolean(itemEnveloped.getItem() != null);
        if(itemEnveloped.getItem() != null) {
            spatiotemporalGeometrySerde.write(kryo, output, itemEnveloped.getItem());
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        byte typeId = input.readByte();
        IndexType indexType = IndexType.fromId(typeId);
        switch (indexType) {
            case STRTREE: {
                int nodeCapacity = input.readInt();
                SpatiotemporalEnvelope bounds = readSpatiotemporalEnvelope(kryo, input, aClass);
                STRTree strTree = new STRTree(nodeCapacity, bounds);
                if(input.readBoolean()) {
                    // if the STRTree is not empty
                    if(input.readBoolean()) {
                        // if the STRTree has been built
                        strTree.setBuilt(true);
                        strTree.setItemsBoundables(null);
                        strTree.setRoot(readSTRNode(kryo, input));
                    } else {
                        // if the STRTree has NOT been built
                        int numItemBoundables = input.readInt();
                        List itemBoundables = new ArrayList(numItemBoundables);
                        for(int i=0; i<numItemBoundables; i++) {
                            itemBoundables.add(readItemBoundable(kryo, input));
                        }
                        strTree.setItemsBoundables(itemBoundables);
                    }
                }
                return strTree;
            }
            case KDBTREE: {
                int maxChildrenPerNode = input.readInt();
                int maxItemsPerNode = input.readInt();
                double underflowFraction = input.readDouble();
                SpatiotemporalEnvelope envelope = readSpatiotemporalEnvelope(kryo, input, aClass);
                int idBase = input.readInt();
                int XIndex = input.readInt();
                int YIndex = input.readInt();
                int TimeIndex = input.readInt();
                int currentOrder = input.readInt();
                int orderBase = input.readInt();
                KDBTree kdbTree = new KDBTree(maxChildrenPerNode, maxItemsPerNode, underflowFraction, envelope, XIndex, YIndex, TimeIndex);
                kdbTree.setIdBase(idBase);
                kdbTree.getCyclicSplitter().setCurrentOrder(currentOrder);
                kdbTree.getCyclicSplitter().setOrderBase(orderBase);
                if(input.readBoolean()) {
                    // if kdbTree is not empty
                    readKDBTree(kryo, input, kdbTree, kdbTree.getCyclicSplitter());
                }
                return kdbTree;
            }
            default: {
                throw new UnsupportedOperationException("This index type is not supported: " + indexType);
            }
        }
    }

    private SpatiotemporalEnvelope readSpatiotemporalEnvelope(Kryo kryo, Input input, Class aClass) {
        if(input.readBoolean()) {
            // if the envelope is not null
            return (SpatiotemporalEnvelope) spatiotemporalGeometrySerde.read(kryo, input, aClass);
        }
        return null;
    }

    private STRNode readSTRNode(Kryo kryo, Input input) {
        int level = input.readInt();
        int numChildren = input.readInt();
        SpatiotemporalEnvelope bounds = readSpatiotemporalEnvelope(kryo, input, SpatiotemporalEnvelope.class);
        STRNode strNode = new STRNode(level, bounds);
        List children = new ArrayList(numChildren);

        byte nodeType = input.readByte();
        switch (nodeType) {
            case NON_LEAF_NODE: {
                for(int i=0; i<numChildren; i++) {
                    children.add(readSTRNode(kryo, input));
                }
                break;
            }
            case LEAF_NODE: {
                for(int i=0; i<numChildren; i++) {
                    children.add(readItemBoundable(kryo, input));
                }
                break;
            }
            default: {
                throw new UnsupportedOperationException("Node Type of STRTree is not supported: " + nodeType);
            }
        }
        strNode.setChildren(children);
        return strNode;
    }

    private ItemBoundable readItemBoundable(Kryo kryo, Input input) {
        Object bounds = null, item = null;
        if(input.readBoolean()) {
            bounds = spatiotemporalGeometrySerde.read(kryo, input, SpatiotemporalEnvelope.class);
        }
        if(input.readBoolean()) {
            item = spatiotemporalGeometrySerde.read(kryo, input, SpatiotemporalGeometry.class);
        }
        return new ItemBoundable(bounds, item);
    }

    private void readKDBTree(Kryo kryo, Input input, KDBTree kdbTree, CyclicSplitter cyclicSplitter) {
        readSplitOrderAndSplittingPlane(input, kdbTree.getRoot(), cyclicSplitter);
        readKDBChildrenOrItems(kryo, input, kdbTree.getRoot(), cyclicSplitter);
    }

    private void readSplitOrderAndSplittingPlane(Input input, KDBNode kdbNode, CyclicSplitter cyclicSplitter) {
        int splitOrder = input.readInt();
        kdbNode.setSplitOrder(splitOrder);
        if(splitOrder == cyclicSplitter.getX()) {
            double splittingPlaneX = input.readDouble();
            kdbNode.setSplittingPlaneX(splittingPlaneX);
        } else if(splitOrder == cyclicSplitter.getY()) {
            double splittingPlaneY = input.readDouble();
            kdbNode.setSplittingPlaneY(splittingPlaneY);
        } else if(splitOrder == cyclicSplitter.getTime()) {
            long time = input.readLong();
            LocalDateTime splittingPlaneTime = TimeUtils.getDateTimeFromTimestamp(time);
            kdbNode.setSplittingPlaneTime(splittingPlaneTime);
        }
    }

    private void readKDBChildrenOrItems(Kryo kryo, Input input, KDBNode kdbNode, CyclicSplitter cyclicSplitter) {
        byte nodeType = input.readByte();
        switch (nodeType) {
            case NON_LEAF_NODE: {
                int numChildren = input.readInt();
                List<KDBNode> children = new ArrayList<>(numChildren);
                for(int i=0; i<numChildren; i++) {
                    children.add(readKDBNode(kryo, input, kdbNode, cyclicSplitter));
                }
                kdbNode.setChildren(children);
                break;
            }
            case LEAF_NODE: {
                int numItems = input.readInt();
                List<ItemEnveloped> items = new ArrayList<>(numItems);
                for(int i=0; i<numItems; i++) {
                    items.add(readItemEnveloped(kryo, input));
                }
                kdbNode.setItems(items);
                break;
            }
            default: {
                throw new UnsupportedOperationException("Node Type of STRTree is not supported: " + nodeType);
            }
        }
    }

    private KDBNode readKDBNode(Kryo kryo, Input input, KDBNode parent, CyclicSplitter cyclicSplitter) {
        SpatiotemporalEnvelope envelope = readSpatiotemporalEnvelope(kryo, input, SpatiotemporalEnvelope.class);
        KDBNode kdbNode = new KDBNode(parent, envelope);
        readSplitOrderAndSplittingPlane(input, kdbNode, cyclicSplitter);
        readKDBChildrenOrItems(kryo, input, kdbNode, cyclicSplitter);
        return kdbNode;
    }

    private ItemEnveloped readItemEnveloped(Kryo kryo, Input input) {
        Object envelope = null, item = null;
        if(input.readBoolean()) {
            envelope = spatiotemporalGeometrySerde.read(kryo, input, SpatiotemporalEnvelope.class);
        }
        if(input.readBoolean()) {
            item = spatiotemporalGeometrySerde.read(kryo, input, SpatiotemporalGeometry.class);
        }
        return new ItemEnveloped(envelope, item);
    }
}
