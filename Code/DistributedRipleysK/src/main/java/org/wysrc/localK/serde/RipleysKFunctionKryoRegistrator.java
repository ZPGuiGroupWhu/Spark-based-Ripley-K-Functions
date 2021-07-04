package org.wysrc.localK.serde;

import com.esotericsoftware.kryo.Kryo;
import org.apache.log4j.Logger;
import org.apache.spark.serializer.KryoRegistrator;
import org.wysrc.localK.geom.SpatiotemporalCircle;
import org.wysrc.localK.geom.SpatiotemporalEnvelope;
import org.wysrc.localK.geom.SpatiotemporalPoint;
import org.wysrc.localK.index.kdbtree.KDBTree;
import org.wysrc.localK.index.strtree.STRTree;
import org.wysrc.localK.partitioner.CuboidPartitioner;
import org.wysrc.localK.partitioner.KDBTreePartitioner;

public class RipleysKFunctionKryoRegistrator implements KryoRegistrator {
    final static Logger log = Logger.getLogger(RipleysKFunctionKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo) {
        SpatiotemporalGeometrySerde spatiotemporalGeometrySerializer = new SpatiotemporalGeometrySerde();
        SpatiotemporalIndexSerde spatiotemporalIndexSerializer = new SpatiotemporalIndexSerde();
        SpatiotemporalPartitionerSerde spatiotemporalPartitionerSerde = new SpatiotemporalPartitionerSerde();

        log.info("Registering custom serializers for spatiotemporal geometry, index and partitioner types.");

        kryo.register(SpatiotemporalPoint.class, spatiotemporalGeometrySerializer);
        kryo.register(SpatiotemporalCircle.class, spatiotemporalGeometrySerializer);
        kryo.register(SpatiotemporalEnvelope.class, spatiotemporalGeometrySerializer);
        kryo.register(STRTree.class, spatiotemporalIndexSerializer);
        kryo.register(KDBTree.class, spatiotemporalIndexSerializer);
        kryo.register(KDBTreePartitioner.class, spatiotemporalPartitionerSerde);
        kryo.register(CuboidPartitioner.class, spatiotemporalPartitionerSerde);
    }
}
