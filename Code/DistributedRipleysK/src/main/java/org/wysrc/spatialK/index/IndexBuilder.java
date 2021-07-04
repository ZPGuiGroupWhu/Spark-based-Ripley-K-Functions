package org.wysrc.spatialK.index;

import org.locationtech.jts.geom.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.wysrc.spatialK.geom.SpatiotemporalEnvelope;
import org.wysrc.spatialK.geom.SpatiotemporalGeometry;
import org.wysrc.spatialK.index.kdbtree.KDBTree;
import org.wysrc.spatialK.index.strtree.STRTree;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * The spatiotemporal index builder for spatiotemporal RDD
 */
public final class IndexBuilder<T extends SpatiotemporalGeometry>
        implements FlatMapFunction<Iterator<T>, SpatiotemporalIndex> {
    IndexType indexType;

    public IndexBuilder(IndexType indexType) {
        this.indexType = indexType;
    }

    @Override
    public Iterator<SpatiotemporalIndex> call(Iterator<T> objectIterator) throws Exception {
        SpatiotemporalIndex spatiotemporalIndex;
        if(indexType == IndexType.STRTree) {
            spatiotemporalIndex = new STRTree();
            while(objectIterator.hasNext()) {
                T object = objectIterator.next();
                spatiotemporalIndex.insert(object.getEnvelopeInternal(), object);
            }
            Set<SpatiotemporalIndex> result = new HashSet<>();
            spatiotemporalIndex.query(new SpatiotemporalEnvelope(new Envelope(0.0, 0.0, 0.0, 0.0), LocalDateTime.now()));
            result.add(spatiotemporalIndex);
            return result.iterator();
        } else if (indexType == IndexType.KDBTree) {
            spatiotemporalIndex = new KDBTree(null);
            while(objectIterator.hasNext()) {
                T object = objectIterator.next();
                spatiotemporalIndex.insert(object.getEnvelopeInternal(), object);
            }
            Set<SpatiotemporalIndex> result = new HashSet<>();
            spatiotemporalIndex.query(new SpatiotemporalEnvelope(new Envelope(0.0, 0.0, 0.0, 0.0), LocalDateTime.now()));
            result.add(spatiotemporalIndex);
            return result.iterator();
        }  else {
            throw new Exception("[SpatiotemporalRDD][spatiotemporalPartitioning] Unknown index type, please check your input.");
        }
    }
}
