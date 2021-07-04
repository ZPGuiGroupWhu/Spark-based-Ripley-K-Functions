package org.wysrc.crossK.index;

import org.locationtech.jts.index.ItemVisitor;
import org.wysrc.crossK.geom.SpatiotemporalEnvelope;

import java.util.List;

/**
 * The basic operations supported by classes implementing  spatiotemporal index algorithms
 */
public interface SpatiotemporalIndex {
    /**
     * Adds a spatiotemporal item to the index.
     *
     * @param itemEnvelope the envelope of the item
     * @param item the item to insert
     */
    void insert(SpatiotemporalEnvelope itemEnvelope, Object item);

    /**
     * Queries the index for all items whose envelopes intersect the query envelope.
     * Note that some kind of indexes may also return the objects which do not in fact intersect the query envelope.
     *
     * @param queryEnvelope the envelope query for
     * @return a list of items found by the query
     */
    List query(SpatiotemporalEnvelope queryEnvelope);

    /**
     * Queries the index for all items whose envelopes intersect the query envelope, and applies an visitor to them,
     * i.e., modifies the existing items in the index.
     *
     * @param queryEnvelope the envelope query for
     * @param visitor a visitor to apply for the items found
     */
    void query(SpatiotemporalEnvelope queryEnvelope, ItemVisitor visitor);

    /**
     * Remove a item from the index
     *
     * @param itemEnvelope the envelope of the item
     * @param item the item to remove
     * @return true if the item was found
     */
    boolean remove(SpatiotemporalEnvelope itemEnvelope, Object item);
}
