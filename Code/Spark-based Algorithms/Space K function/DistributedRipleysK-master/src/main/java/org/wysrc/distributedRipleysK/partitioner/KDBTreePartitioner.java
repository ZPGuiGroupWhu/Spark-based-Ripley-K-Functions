package org.wysrc.distributedRipleysK.partitioner;

import org.wysrc.distributedRipleysK.geom.SpatiotemporalEnvelope;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalGeometry;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalPoint;
import org.wysrc.distributedRipleysK.index.kdbtree.KDBTree;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
//            try {
//                partitionCount.put(index, partitionCount.get(index) + 1);
//                result.add(new Tuple2<>(index, spatiotemporalObject));
//            } catch (Exception e) {
//                //why index can be -1?It means the point can not find a partitioner.
//                //throw new RuntimeException("index:" + index + "," + "size:" + partitionCount.size());
//            }
            partitionCount.put(index, partitionCount.get(index) + 1);
            result.add(new Tuple2<>(index, spatiotemporalObject));
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
