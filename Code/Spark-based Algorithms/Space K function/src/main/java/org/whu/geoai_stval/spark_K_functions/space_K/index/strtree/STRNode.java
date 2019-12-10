package org.whu.geoai_stval.spark_K_functions.space_K.index.strtree;

import com.vividsolutions.jts.index.strtree.Boundable;
import com.vividsolutions.jts.util.Assert;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalEnvelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class STRNode implements Boundable, Serializable {
    private List children = new ArrayList();
    private SpatiotemporalEnvelope bounds;
    private int level;

    /**
     * Default constructor required for serialization.
     */
    public STRNode() {
    }

    /**
     * Constructs an STRNode at the given level in the tree.
     *
     * @param level 0 if the node is a leaf, 1 if it is a parent of a leaf, and so on;
     * the root will have the highest level.
     */
    public STRNode(int level) {
        this(level, null);
    }

    public STRNode(int level, SpatiotemporalEnvelope bounds) {
        this.level = level;
        if(bounds == null) {
            this.bounds = null;
        } else {
            this.bounds = new SpatiotemporalEnvelope(bounds);
        }
    }

    /**
     * @return children of this node, or if it is a leaf, return the real data
     */
    public List getChildren() {
        return children;
    }

    public void setChildren(List children) {
        this.children = children;
    }

    public SpatiotemporalEnvelope getBounds() {
        if(bounds == null) {
            bounds = computeBounds();
        }
        return bounds;
    }

    public void setBounds(SpatiotemporalEnvelope bounds) {
        this.bounds = bounds;
    }

    private SpatiotemporalEnvelope computeBounds() {
        SpatiotemporalEnvelope bounds = null;
        for(Object child: children) {
            Boundable childBoundable = (Boundable) child;
            if(bounds == null) {
                bounds = new SpatiotemporalEnvelope((SpatiotemporalEnvelope) childBoundable.getBounds());
            } else {
                bounds.expandToInclude((SpatiotemporalEnvelope) childBoundable.getBounds());
            }
        }
        return bounds;
    }

    public int getLevel() {
        return level;
    }

    public int size() {
        return children.size();
    }

    public boolean isEmpty() {
        return children.isEmpty();
    }

    public void addChild(Boundable child) {
        Assert.isTrue(child != null);
        children.add(child);
    }
}
