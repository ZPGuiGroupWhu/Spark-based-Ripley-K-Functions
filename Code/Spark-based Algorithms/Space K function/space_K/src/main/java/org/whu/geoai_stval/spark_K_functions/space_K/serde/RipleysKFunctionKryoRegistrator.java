package org.whu.geoai_stval.spark_K_functions.space_K.serde;

import com.esotericsoftware.kryo.Kryo;
import org.apache.log4j.Logger;
import org.apache.spark.serializer.KryoRegistrator;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalCircle;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalEnvelope;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalPoint;
import org.whu.geoai_stval.spark_K_functions.space_K.index.kdbtree.KDBTree;
import org.whu.geoai_stval.spark_K_functions.space_K.index.strtree.STRTree;
import org.whu.geoai_stval.spark_K_functions.space_K.partitioner.CuboidPartitioner;
import org.whu.geoai_stval.spark_K_functions.space_K.partitioner.KDBTreePartitioner;

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
