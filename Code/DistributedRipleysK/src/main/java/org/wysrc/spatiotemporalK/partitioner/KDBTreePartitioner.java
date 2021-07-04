package org.wysrc.spatiotemporalK.partitioner;

import org.wysrc.spatiotemporalK.geom.SpatiotemporalEnvelope;
import org.wysrc.spatiotemporalK.geom.SpatiotemporalGeometry;
import org.wysrc.spatiotemporalK.index.kdbtree.KDBTree;
import scala.Tuple2;

import java.util.*;

public class KDBTreePartitioner extends SpatiotemporalPartitioner {
    private final KDBTree kdbTree;

    public KDBTreePartitioner(KDBTree kdbTree) {
        super(PartitionerType.KDBTree, kdbTree.getLeafEnvelopes(true));
        this.kdbTree = kdbTree;
    }

    @Override
    public <T extends SpatiotemporalGeometry> Iterator<Tuple2<Integer, T>> divideObject(T spatiotemporalObject) {
        Objects.requireNonNull(spatiotemporalObject, "Spatiotemporal Object cannot be null!");
        List<SpatiotemporalEnvelope> leafEnvelopes = kdbTree.queryLeafEnvelope(spatiotemporalObject);
        Set<Tuple2<Integer, T>> result = new HashSet<>();
        for(SpatiotemporalEnvelope leafEnvelope: leafEnvelopes) {
            result.add(new Tuple2<>(leafEnvelope.id, spatiotemporalObject));
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
