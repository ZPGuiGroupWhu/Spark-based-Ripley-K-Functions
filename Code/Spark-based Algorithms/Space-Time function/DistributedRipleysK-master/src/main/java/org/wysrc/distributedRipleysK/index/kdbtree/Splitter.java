package org.wysrc.distributedRipleysK.index.kdbtree;

import org.wysrc.distributedRipleysK.geom.SpatiotemporalEnvelope;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalGeometry;

import java.util.List;

public interface Splitter {
    /**
     * Splits the envelope from a node of KDB-Tree
     *
     * @param envelope the envelope to split
     * @return the sub-envelopes
     */
    SpatiotemporalEnvelope[] splitEnvelope(SpatiotemporalEnvelope envelope);

    /**
     * Splits the items from a data node of KDB-Tree
     * @param items the items to split
     * @return the sub-items
     */
    List<ItemEnveloped>[] splitItems(List<ItemEnveloped> items);
}