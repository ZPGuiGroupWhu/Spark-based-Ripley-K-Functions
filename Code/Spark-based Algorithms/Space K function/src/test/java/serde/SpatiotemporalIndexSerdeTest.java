package serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.junit.Assert;
import org.junit.Test;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalEnvelope;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalPoint;
import org.whu.geoai_stval.spark_K_functions.space_K.index.SpatiotemporalIndex;
import org.whu.geoai_stval.spark_K_functions.space_K.index.kdbtree.KDBTree;
import org.whu.geoai_stval.spark_K_functions.space_K.index.strtree.STRTree;
import org.whu.geoai_stval.spark_K_functions.space_K.serde.SpatiotemporalIndexSerde;

import java.io.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;

import static org.hamcrest.core.Is.is;

public class SpatiotemporalIndexSerdeTest {
    private final Kryo kryo = new Kryo();
    private final SpatiotemporalIndexSerde spatiotemporalIndexSerde = new SpatiotemporalIndexSerde();
    private final int numGeometry = 1000000;

    @Test
    public void test() throws IOException, ClassNotFoundException {
        kryo.register(STRTree.class, spatiotemporalIndexSerde);
        kryo.register(KDBTree.class, spatiotemporalIndexSerde);

//        testCorrectness(numGeometry, STRTree.class);
//        compareSize(numGeometry, STRTree.class);
//        compareTime(numGeometry, STRTree.class);

        testCorrectness(numGeometry, KDBTree.class);
        compareSize(numGeometry, KDBTree.class);
        compareTime(numGeometry, KDBTree.class);
    }

    private void testCorrectness(int size, Class aClass) throws IOException {
        System.out.println("\n==== Start Correctness Test of " + aClass.toString() + " ====");
        SpatiotemporalIndex index = generateIndex(size, aClass);
        queryIndex(index, null);

        SpatiotemporalIndex serdeIndex = deserializeWithKryo(serializeWithKryo(index));
        // test query full envelope
        Assert.assertThat(queryIndex(index, null), is(queryIndex(serdeIndex, null)));

        // test query partial envelope
        SpatiotemporalEnvelope spatiotemporalEnvelope = new SpatiotemporalEnvelope(new Envelope(-90, 90, -45, 45),
                LocalDateTime.now().minus(1, ChronoUnit.SECONDS), LocalDateTime.now());
        Assert.assertThat(queryIndex(index, spatiotemporalEnvelope), is(queryIndex(serdeIndex, spatiotemporalEnvelope)));

        spatiotemporalEnvelope = new SpatiotemporalEnvelope(new Envelope(0, 90, -15, 30),
                LocalDateTime.now().minus(1, ChronoUnit.SECONDS), LocalDateTime.now());
        Assert.assertThat(queryIndex(index, spatiotemporalEnvelope), is(queryIndex(serdeIndex, spatiotemporalEnvelope)));

        System.out.println("\n==== Completed Correctness Test of " + aClass.toString() + " ====");
    }

    private void compareSize(int size, Class aClass) throws IOException {
        System.out.println("\n==== test Size of " + aClass.toString() + " ====");
        SpatiotemporalIndex index = generateIndex(size, aClass);
        queryIndex(index, null);

        byte[] noKryo = serializeWithNOKryo(index);
        byte[] withKryo = serializeWithKryo(index);

        System.out.println("Original size: " + noKryo.length);
        System.out.println("With Kryo size: " + withKryo.length);
        System.out.println("Proportion: " + withKryo.length/(double) noKryo.length * 100 + " %");
    }

    private void compareTime(int size, Class aClass) throws IOException, ClassNotFoundException {
        System.out.println("\n==== test Serde time of " + aClass.toString() + " ====");
        SpatiotemporalIndex index = generateIndex(size, aClass);
        queryIndex(index, null);
        System.out.println("Data Size: " + size);
        double before, after;

        before = System.currentTimeMillis();
        byte[] noKryo = serializeWithNOKryo(index);
        after = System.currentTimeMillis();
        System.out.println("Original Serialization time: " + (after - before) + " ms");

        before = System.currentTimeMillis();
        deserializeWithNOKryo(noKryo);
        after = System.currentTimeMillis();
        System.out.println("Original Deserialization time: " + (after - before) + " ms");

        before = System.currentTimeMillis();
        byte[] withKryo = serializeWithKryo(index);
        after = System.currentTimeMillis();
        System.out.println("With Kryo Serialization time: " + (after - before) + " ms");

        before = System.currentTimeMillis();
        deserializeWithKryo(withKryo);
        after = System.currentTimeMillis();
        System.out.println("With Kryo Deserialization time: " + (after - before) + " ms");
    }

    private SpatiotemporalIndex generateIndex(int numGeometry, Class aClass) {
        Random random = new Random();
        SpatiotemporalIndex index;
        if(aClass == STRTree.class) {
            index = new STRTree();
        } else if(aClass == KDBTree.class) {
            index = new KDBTree(new SpatiotemporalEnvelope(new Envelope(-180, 180, -90, 90),
                    LocalDateTime.now().minus(30, ChronoUnit.MINUTES),
                    LocalDateTime.now().plus(30, ChronoUnit.MINUTES)));
        } else {
            return null;
        }

        for(int i=0; i<numGeometry; i++) {
            SpatiotemporalPoint spatiotemporalPoint = new SpatiotemporalPoint(new Coordinate(random.nextDouble()*360-180,
                    random.nextDouble()*180-90), LocalDateTime.now());
            index.insert(spatiotemporalPoint.getEnvelopeInternal(), spatiotemporalPoint);
        }

        return index;
    }

    private List queryIndex(SpatiotemporalIndex index, SpatiotemporalEnvelope spatiotemporalEnvelope) {
        if (index instanceof STRTree) {
            STRTree strTree = (STRTree) index;
            if (spatiotemporalEnvelope == null) {
                spatiotemporalEnvelope = new SpatiotemporalEnvelope(new Envelope(-180, 180, -90, 90),
                        LocalDateTime.now().minus(30, ChronoUnit.SECONDS), LocalDateTime.now());
            }
            return strTree.query(spatiotemporalEnvelope);
        } else if(index instanceof KDBTree) {
            KDBTree kdbTree = (KDBTree) index;
            if(spatiotemporalEnvelope == null) {
                spatiotemporalEnvelope = new SpatiotemporalEnvelope(new Envelope(-180, 180, -90, 90),
                        LocalDateTime.now().minus(30, ChronoUnit.SECONDS), LocalDateTime.now());
            }
            return kdbTree.query(spatiotemporalEnvelope);
        } else {
            throw new UnsupportedOperationException("The index type is not supported: " + index.getClass().getName());
        }
    }

    private byte[] serializeWithKryo(SpatiotemporalIndex index) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        kryo.writeClassAndObject(output, index);
        output.close();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private SpatiotemporalIndex deserializeWithKryo(byte[] byteArray) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        Input input = new Input(inputStream);
        input.close();
        SpatiotemporalIndex deserObject = (SpatiotemporalIndex) kryo.readClassAndObject(input);
        inputStream.close();
        return deserObject;
    }

    private byte[] serializeWithNOKryo(SpatiotemporalIndex index) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutput output = new ObjectOutputStream(outputStream);
        output.writeObject(index);
        output.close();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private SpatiotemporalIndex deserializeWithNOKryo(byte[] byteArray) throws IOException, ClassNotFoundException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        ObjectInput input = new ObjectInputStream(inputStream);
        SpatiotemporalIndex deserObject = (SpatiotemporalIndex) input.readObject();
        input.close();
        inputStream.close();
        return deserObject;
    }
}
