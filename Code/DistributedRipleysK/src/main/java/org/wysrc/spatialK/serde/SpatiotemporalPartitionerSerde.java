package org.wysrc.spatialK.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.wysrc.spatialK.geom.SpatiotemporalEnvelope;
import org.wysrc.spatialK.index.kdbtree.KDBTree;
import org.wysrc.spatialK.partitioner.CuboidPartitioner;
import org.wysrc.spatialK.partitioner.KDBTreePartitioner;

import java.util.ArrayList;
import java.util.List;

public class SpatiotemporalPartitionerSerde extends Serializer {
    private static final Logger log = Logger.getLogger(SpatiotemporalPartitionerSerde.class);
    private SpatiotemporalIndexSerde spatiotemporalIndexSerde;

    public SpatiotemporalPartitionerSerde() {
        super();
        this.spatiotemporalIndexSerde = new SpatiotemporalIndexSerde();
    }

    public SpatiotemporalPartitionerSerde(SpatiotemporalIndexSerde spatiotemporalIndexSerde) {
        super();
        this.spatiotemporalIndexSerde = spatiotemporalIndexSerde;
    }

    private enum SpatiotemporalPartitionerType {
        KDBTREE_PARTITIONER(0),
        CUBOID_PARTITIONER(1);

        private final int id;

        SpatiotemporalPartitionerType(int id) {
            this.id = id;
        }

        public static SpatiotemporalPartitionerType fromId(int id) {
            for(SpatiotemporalPartitionerType type: values()) {
                if(type.id == id) {
                    return type;
                }
            }
            return null;
        }
    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        if(object instanceof KDBTreePartitioner) {
            writeType(output, SpatiotemporalPartitionerType.KDBTREE_PARTITIONER);
            KDBTree kdbTree = ((KDBTreePartitioner) object).getKdbTree();
            output.writeBoolean(kdbTree != null);
            if(kdbTree != null) {
                spatiotemporalIndexSerde.write(kryo, output, kdbTree);
            }
        } else if(object instanceof CuboidPartitioner) {
            writeType(output, SpatiotemporalPartitionerType.CUBOID_PARTITIONER);
            List<SpatiotemporalEnvelope> cuboids = ((CuboidPartitioner) object).getCuboids();
            output.writeBoolean(cuboids != null);
            if(cuboids != null) {
                writeCuboids(kryo, output, cuboids);
            }
        } else {
            throw new UnsupportedOperationException("Cannot serialize object of type: " + object.getClass().getName());
        }
    }

    private void writeType(Output output, SpatiotemporalPartitionerType type) {
        output.writeByte((byte) type.id);
    }

    private void writeCuboids(Kryo kryo, Output output, List<SpatiotemporalEnvelope> cuboids) {
        output.writeInt(cuboids.size());
        for(SpatiotemporalEnvelope cuboid: cuboids) {
            spatiotemporalIndexSerde.getSpatiotemporalGeometrySerde().write(kryo, output, cuboid);
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        byte typeId = input.readByte();
        SpatiotemporalPartitionerType partitionerType = SpatiotemporalPartitionerType.fromId(typeId);
        switch (partitionerType) {
            case KDBTREE_PARTITIONER: {
                if(input.readBoolean()) {
                    KDBTree kdbTree = (KDBTree) spatiotemporalIndexSerde.read(kryo, input, KDBTree.class);
                    return new KDBTreePartitioner(kdbTree);
                } else {
                    return new KDBTreePartitioner(null);
                }
            }
            case CUBOID_PARTITIONER: {
                if(input.readBoolean()) {
                    List<SpatiotemporalEnvelope> cuboids = readCuboids(kryo, input);
                    return new CuboidPartitioner(cuboids);
                } else {
                    return new CuboidPartitioner(null);
                }
            }
            default: {
                throw new UnsupportedOperationException("This partitioner type is not supported: " + partitionerType);
            }
        }
    }

    private List<SpatiotemporalEnvelope> readCuboids(Kryo kryo, Input input) {
        int numCuboids = input.readInt();
        List<SpatiotemporalEnvelope> cuboids = new ArrayList<>();
        for(int i=0; i<numCuboids; i++) {
            cuboids.add((SpatiotemporalEnvelope) spatiotemporalIndexSerde.getSpatiotemporalGeometrySerde().
                    read(kryo, input, SpatiotemporalEnvelope.class));
        }
        return cuboids;
    }
}
