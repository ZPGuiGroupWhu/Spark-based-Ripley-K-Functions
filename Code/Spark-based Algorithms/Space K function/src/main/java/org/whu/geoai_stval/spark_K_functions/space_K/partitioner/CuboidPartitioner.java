package org.whu.geoai_stval.spark_K_functions.space_K.partitioner;

import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalEnvelope;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalGeometry;
import scala.Tuple2;

import java.util.*;

/**
 * The partitioner for join between non-spatiotemporally-partitioned RDD and indexed RDD
 */
public class CuboidPartitioner extends SpatiotemporalPartitioner {
    public CuboidPartitioner(List<SpatiotemporalEnvelope> cuboids) {
        super(PartitionerType.Cuboid, cuboids);
    }

    @Override
    public <T extends SpatiotemporalGeometry> Iterator<Tuple2<Integer, T>> divideObject(T spatiotemporalObject) {
        Objects.requireNonNull(spatiotemporalObject, "Spatiotemporal Object cannot be null!");

        final SpatiotemporalEnvelope objectEnvelope = spatiotemporalObject.getEnvelopeInternal();
        Set<Tuple2<Integer, T>> result = new HashSet<>();
        for(SpatiotemporalEnvelope cuboid: cuboids) {
            if(cuboid.intersects(objectEnvelope) || objectEnvelope.covers(cuboid)) {
                result.add(new Tuple2<>(cuboid.id, spatiotemporalObject));
            }
        }
        return result.iterator();
    }

    @Override
    public int numPartitions() {
        return cuboids.size();
    }
}
