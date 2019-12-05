package serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.junit.Assert;
import org.junit.Test;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalEnvelope;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalPoint;
import org.wysrc.distributedRipleysK.index.kdbtree.KDBTree;
import org.wysrc.distributedRipleysK.partitioner.CuboidPartitioner;
import org.wysrc.distributedRipleysK.partitioner.KDBTreePartitioner;
import org.wysrc.distributedRipleysK.partitioner.SpatiotemporalPartitioner;
import org.wysrc.distributedRipleysK.serde.SpatiotemporalPartitionerSerde;

import java.io.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.core.Is.is;

public class SpatiotemporalPartitionerSerdeTest {
    private final Kryo kryo = new Kryo();
    private final SpatiotemporalPartitionerSerde spatiotemporalPartitionerSerde = new SpatiotemporalPartitionerSerde();
    private final int numCuboids = 1000000;

    @Test
    public void test() throws IOException, ClassNotFoundException {
        kryo.register(KDBTreePartitioner.class, spatiotemporalPartitionerSerde);
        kryo.register(CuboidPartitioner.class, spatiotemporalPartitionerSerde);

        testCorrectness(numCuboids, KDBTreePartitioner.class);
        compareSize(numCuboids, KDBTreePartitioner.class);
        compareTime(numCuboids, KDBTreePartitioner.class);

        testCorrectness(numCuboids, CuboidPartitioner.class);
        compareSize(numCuboids, CuboidPartitioner.class);
        compareTime(numCuboids, CuboidPartitioner.class);
    }

    private void testCorrectness(int size, Class aClass) throws IOException {
        System.out.println("\n==== Start Correctness Test of " + aClass.toString() + " ====");
        SpatiotemporalPartitioner partitioner = generatePartitioner(size, aClass);
        SpatiotemporalPartitioner serdePartitioner = deserializeWithKryo(serializeWithKryo(partitioner));

        Assert.assertThat(partitioner.getCuboids(), is(serdePartitioner.getCuboids()));

        System.out.println("\n==== Completed Correctness Test of " + aClass.toString() + " ====");
    }

    private void compareSize(int size, Class aClass) throws IOException {
        System.out.println("\n==== test Size of " + aClass.toString() + " ====");
        SpatiotemporalPartitioner partitioner = generatePartitioner(size, aClass);

        byte[] noKryo = serializeWithNOKryo(partitioner);
        byte[] withKryo = serializeWithKryo(partitioner);

        System.out.println("Original size: " + noKryo.length);
        System.out.println("With Kryo size: " + withKryo.length);
        System.out.println("Proportion: " + withKryo.length/(double) noKryo.length * 100 + " %");
    }

    private void compareTime(int size, Class aClass) throws IOException, ClassNotFoundException {
        System.out.println("\n==== test Serde time of " + aClass.toString() + " ====");
        SpatiotemporalPartitioner partitioner = generatePartitioner(size, aClass);
        System.out.println("Data Size: " + size);
        double before, after;

        before = System.currentTimeMillis();
        byte[] noKryo = serializeWithNOKryo(partitioner);
        after = System.currentTimeMillis();
        System.out.println("Original Serialization time: " + (after - before) + " ms");

        before = System.currentTimeMillis();
        deserializeWithNOKryo(noKryo);
        after = System.currentTimeMillis();
        System.out.println("Original Deserialization time: " + (after - before) + " ms");

        before = System.currentTimeMillis();
        byte[] withKryo = serializeWithKryo(partitioner);
        after = System.currentTimeMillis();
        System.out.println("With Kryo Serialization time: " + (after - before) + " ms");

        before = System.currentTimeMillis();
        deserializeWithKryo(withKryo);
        after = System.currentTimeMillis();
        System.out.println("With Kryo Deserialization time: " + (after - before) + " ms");
    }

    private SpatiotemporalPartitioner generatePartitioner(int size, Class aClass) {
        Random random = new Random();
        SpatiotemporalPartitioner partitioner;

        if(aClass == KDBTreePartitioner.class) {
            KDBTree kdbTree = new KDBTree(new SpatiotemporalEnvelope(new Envelope(-180, 180, -90, 90),
                    LocalDateTime.now().minus(30, ChronoUnit.MINUTES),
                    LocalDateTime.now().plus(30, ChronoUnit.MINUTES)));
            for(int i=0; i<size; i++) {
                SpatiotemporalPoint spatiotemporalPoint = new SpatiotemporalPoint(new Coordinate(random.nextDouble()*360-180,
                        random.nextDouble()*180-90), LocalDateTime.now());
                kdbTree.insert(spatiotemporalPoint.getEnvelopeInternal(), spatiotemporalPoint);
            }
            partitioner = new KDBTreePartitioner(kdbTree);
        } else if(aClass == CuboidPartitioner.class) {
            List<SpatiotemporalEnvelope> cuboids = new ArrayList<>();
            for(int i=0; i<size; i++) {
                SpatiotemporalEnvelope cuboid = new SpatiotemporalEnvelope(new Envelope(random.nextDouble()*180-180,
                        random.nextDouble()*180, random.nextDouble()*90-90, random.nextDouble()*90),
                        LocalDateTime.now().minus(5, ChronoUnit.SECONDS), LocalDateTime.now().plus(5, ChronoUnit.SECONDS));
                cuboids.add(cuboid);
            }
            partitioner = new CuboidPartitioner(cuboids);
        } else {
            return null;
        }

        return partitioner;
    }

    private byte[] serializeWithKryo(SpatiotemporalPartitioner partitioner) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        kryo.writeClassAndObject(output, partitioner);
        output.close();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private SpatiotemporalPartitioner deserializeWithKryo(byte[] byteArray) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        Input input = new Input(inputStream);
        input.close();
        SpatiotemporalPartitioner deserObject = (SpatiotemporalPartitioner) kryo.readClassAndObject(input);
        inputStream.close();
        return deserObject;
    }

    private byte[] serializeWithNOKryo(SpatiotemporalPartitioner index) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutput output = new ObjectOutputStream(outputStream);
        output.writeObject(index);
        output.close();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private SpatiotemporalPartitioner deserializeWithNOKryo(byte[] byteArray) throws IOException, ClassNotFoundException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        ObjectInput input = new ObjectInputStream(inputStream);
        SpatiotemporalPartitioner deserObject = (SpatiotemporalPartitioner) input.readObject();
        input.close();
        inputStream.close();
        return deserObject;
    }
}
