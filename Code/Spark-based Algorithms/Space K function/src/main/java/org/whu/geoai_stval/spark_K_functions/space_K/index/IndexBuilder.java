package org.whu.geoai_stval.spark_K_functions.space_K.index;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalEnvelope;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalGeometry;
import org.whu.geoai_stval.spark_K_functions.space_K.index.kdbtree.KDBTree;
import org.whu.geoai_stval.spark_K_functions.space_K.index.strtree.STRTree;

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
        } else if (indexType == IndexType.KDBTree) {
            spatiotemporalIndex = new KDBTree(null);
        } else {
            throw new Exception("[SpatiotemporalRDD][spatiotemporalPartitioning] Unknown index type, please check your input.");
        }
        while(objectIterator.hasNext()) {
            T object = objectIterator.next();
            spatiotemporalIndex.insert(object.getEnvelopeInternal(), object);
        }
        Set<SpatiotemporalIndex> result = new HashSet<>();
        spatiotemporalIndex.query(new SpatiotemporalEnvelope(new Envelope(0.0, 0.0, 0.0, 0.0), LocalDateTime.now()));
        result.add(spatiotemporalIndex);

        return result.iterator();
    }
}
