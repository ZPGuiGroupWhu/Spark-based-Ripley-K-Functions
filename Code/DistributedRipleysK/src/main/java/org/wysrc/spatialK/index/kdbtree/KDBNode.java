package org.wysrc.spatialK.index.kdbtree;

import org.locationtech.jts.util.Assert;
import org.wysrc.spatialK.geom.SpatiotemporalEnvelope;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * KDBNode of KDB-Tree, non-leaf nodes and leaf nodes can be distinguished based on type of their children
 */
public class KDBNode implements Enveloped, Serializable {
    private List<KDBNode> children = new ArrayList<>();
    private List<ItemEnveloped> items = new ArrayList<>();
    private KDBNode parent = null;
    private SpatiotemporalEnvelope envelope =null;
    private int splitOrder = -1;
    private double splittingPlaneX;
    private double splittingPlaneY;
    private LocalDateTime splittingPlaneTime;

    public KDBNode(KDBNode parent, SpatiotemporalEnvelope envelope) {
        this.parent = parent;
        if(envelope == null) {
            this.envelope = null;
        } else {
            this.envelope = new SpatiotemporalEnvelope(envelope);
        }
    }

    public List<KDBNode> getChildren() {
        return children;
    }

    public void setChildren(List<KDBNode> children) {
        this.children = children;
    }

    public void addChild(KDBNode child) {
        Assert.isTrue(child != null);
        this.children.add(child);
    }

    public boolean removeChild(KDBNode child) {
        return this.children.remove(child);
    }

    public List<ItemEnveloped> getItems() {
        return items;
    }

    public void setItems(List<ItemEnveloped> items) {
        this.items = items;
    }

    public void addItem(ItemEnveloped item) {
        Assert.isTrue(item != null);
        this.items.add(item);
    }

    public boolean removeItem(ItemEnveloped item) {
        return this.items.remove(item);
    }

    public KDBNode getParent() {
        return parent;
    }

    public void setParent(KDBNode parent) {
        this.parent = parent;
    }

    public SpatiotemporalEnvelope getEnvelope() {
        if(envelope == null) {
            envelope = computeEnvelope();
        }
        return envelope;
    }

    public void setEnvelope(SpatiotemporalEnvelope envelope) {
        this.envelope = envelope;
    }

    private SpatiotemporalEnvelope computeEnvelope() {
        SpatiotemporalEnvelope envelope = null;
        if(!isLeaf()) {
            for (KDBNode child : getChildren()) {
                if (envelope == null) {
                    envelope = new SpatiotemporalEnvelope(child.getEnvelope());
                } else {
                    envelope.expandToInclude(child.getEnvelope());
                }
            }
        } else {
            for(ItemEnveloped item: getItems()) {
                if (envelope == null) {
                    envelope = new SpatiotemporalEnvelope((SpatiotemporalEnvelope) item.getEnvelope());
                } else {
                    envelope.expandToInclude((SpatiotemporalEnvelope) item.getEnvelope());
                }
            }
        }
        return envelope;
    }

    public int getSplitOrder() {
        return splitOrder;
    }

    public void setSplitOrder(int splitOrder) {
        this.splitOrder = splitOrder;
    }

    public double getSplittingPlaneX() {
        return splittingPlaneX;
    }

    public void setSplittingPlaneX(double splittingPlaneX) {
        this.splittingPlaneX = splittingPlaneX;
    }

    public double getSplittingPlaneY() {
        return splittingPlaneY;
    }

    public void setSplittingPlaneY(double splittingPlaneY) {
        this.splittingPlaneY = splittingPlaneY;
    }

    public LocalDateTime getSplittingPlaneTime() {
        return splittingPlaneTime;
    }

    public void setSplittingPlaneTime(LocalDateTime splittingPlaneTime) {
        this.splittingPlaneTime = splittingPlaneTime;
    }

    public void setSplittingPlane(CyclicSplitter cyclicSplitter) {
        if(this.splitOrder == cyclicSplitter.getX()) {
            this.splittingPlaneX = cyclicSplitter.getSplittingPlaneX();
        } else if(this.splitOrder == cyclicSplitter.getY()) {
            this.splittingPlaneY = cyclicSplitter.getSplittingPlaneY();
        } else if(this.splitOrder == cyclicSplitter.getTime()) {
            this.splittingPlaneTime = cyclicSplitter.getSplittingPlaneTime();
        }
    }

    public boolean isLeaf() {
        return this.children.isEmpty();
    }
}
