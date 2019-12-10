package org.whu.geoai_stval.spark_K_functions.space_time_K.partitioner;

import org.whu.geoai_stval.spark_K_functions.space_time_K.geom.SpatiotemporalEnvelope;
import org.whu.geoai_stval.spark_K_functions.space_time_K.geom.SpatiotemporalGeometry;
import org.whu.geoai_stval.spark_K_functions.space_time_K.geom.SpatiotemporalPoint;
import org.whu.geoai_stval.spark_K_functions.space_time_K.index.kdbtree.KDBTree;
import scala.Tuple2;

import java.util.*;

public class KDBTreePartitioner extends SpatiotemporalPartitioner {
    private final KDBTree kdbTree;
    private Map<Integer, Integer> partitionCount;

    public KDBTreePartitioner(KDBTree kdbTree) {
        super(PartitionerType.KDBTree, kdbTree.getLeafEnvelopes(true));
        this.kdbTree = kdbTree;
        this.partitionCount = new HashMap<>(cuboids.size());
        for(int i=0; i<this.cuboids.size(); i++) {
            partitionCount.put(i, 0);
        }
    }

    @Override
    public <T extends SpatiotemporalGeometry> Iterator<Tuple2<Integer, T>> divideObject(T spatiotemporalObject) {
        Objects.requireNonNull(spatiotemporalObject, "Spatiotemporal Object cannot be null!");
        List<SpatiotemporalEnvelope> leafEnvelopes = kdbTree.queryLeafEnvelope(spatiotemporalObject);
        Set<Tuple2<Integer, T>> result = new HashSet<>();

        if(spatiotemporalObject instanceof SpatiotemporalPoint) {
            int minCount = Integer.MAX_VALUE, index = -1;
            for(SpatiotemporalEnvelope leafEnvelope: leafEnvelopes) {
                int currentCount = partitionCount.get(leafEnvelope.id);
                if(currentCount < minCount) {
                    minCount = currentCount;
                    index = leafEnvelope.id;
                }
            }
            result.add(new Tuple2<>(index, spatiotemporalObject));
            partitionCount.put(index, partitionCount.get(index) + 1);
//            result.add(new Tuple2<>(leafEnvelopes.get(0).id, spatiotemporalObject));
        } else {
            for(SpatiotemporalEnvelope leafEnvelope: leafEnvelopes) {
                result.add(new Tuple2<>(leafEnvelope.id, spatiotemporalObject));
            }
        }

        return result.iterator();
    }

    @Override
    public int numPartitions() {
        return cuboids.size();
    }

    public KDBTree getKdbTree() {
        return kdbTree;
    }
}
