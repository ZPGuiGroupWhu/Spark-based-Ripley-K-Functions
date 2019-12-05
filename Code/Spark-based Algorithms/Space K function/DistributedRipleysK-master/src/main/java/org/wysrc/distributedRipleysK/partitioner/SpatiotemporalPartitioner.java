package org.wysrc.distributedRipleysK.partitioner;

import org.apache.spark.Partitioner;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalEnvelope;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalGeometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * The spatiotemporal partitioner for spatiotemporal RDD
 */
public abstract class SpatiotemporalPartitioner extends Partitioner implements Serializable {
    protected final PartitionerType partitionerType;
    protected final List<SpatiotemporalEnvelope> cuboids;

    protected SpatiotemporalPartitioner(PartitionerType partitionerType, List<SpatiotemporalEnvelope> cuboids) {
        this.partitionerType = partitionerType;
        this.cuboids = cuboids;
    }

    public abstract <T extends SpatiotemporalGeometry> Iterator<Tuple2<Integer, T>> divideObject(T spatiotemporalObject) throws Exception;

    public PartitionerType getPartitionerType() {
        return partitionerType;
    }

    public List<SpatiotemporalEnvelope> getCuboids() {
        return cuboids;
    }

    @Override
    public int getPartition(Object key) {
        return (int) key;
    }
}
