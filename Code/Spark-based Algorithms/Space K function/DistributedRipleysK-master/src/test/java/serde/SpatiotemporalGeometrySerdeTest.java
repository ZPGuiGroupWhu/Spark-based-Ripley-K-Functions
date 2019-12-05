package serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import org.junit.Assert;
import org.junit.Test;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalCircle;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalEnvelope;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalGeometry;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalPoint;
import org.wysrc.distributedRipleysK.serde.SpatiotemporalGeometrySerde;
import org.wysrc.distributedRipleysK.utils.TimeUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SpatiotemporalGeometrySerdeTest {
    private final Kryo kryo = new Kryo();
    private final SpatiotemporalGeometrySerde spatiotemporalGeometrySerde = new SpatiotemporalGeometrySerde();
    private final int testCorrectSize = 1000;
    private final int compareSizeSize = 10000;
    private final int compareTimeSize = 10000;
    private final int iteration = 100;

    @Test
    public void test() throws IOException, ClassNotFoundException {
        kryo.register(SpatiotemporalPoint.class, spatiotemporalGeometrySerde);
        kryo.register(SpatiotemporalCircle.class, spatiotemporalGeometrySerde);
        kryo.register(SpatiotemporalEnvelope.class, spatiotemporalGeometrySerde);

//        testCorrectness(testCorrectSize, SpatiotemporalPoint.class);
//        testCorrectness(testCorrectSize, SpatiotemporalCircle.class);
//        testCorrectness(testCorrectSize, SpatiotemporalEnvelope.class);

        compareSize(compareSizeSize, SpatiotemporalPoint.class);
        compareSize(compareSizeSize, SpatiotemporalCircle.class);
        compareSize(compareSizeSize, SpatiotemporalEnvelope.class);

        compareTime(iteration, compareTimeSize, SpatiotemporalPoint.class);
//        compareTime(iteration, compareTimeSize, SpatiotemporalCircle.class);
//        compareTime(iteration, compareTimeSize, SpatiotemporalEnvelope.class);
    }

    private void testCorrectness(int size, Class aClass) throws IOException {
        Object[] testObject = generateSpatiotemporalGeometry(size, aClass);

        System.out.println("\n==== Start Correctness Test of " + aClass.toString() + " ====");

        for(int i=0; i<testObject.length; i++) {
            Object serdeObject = deserializeWithKryo(serializeWithKryo(testObject[i]));
            Assert.assertEquals(testObject[i], serdeObject);
        }

        if(aClass == SpatiotemporalPoint.class || aClass == SpatiotemporalCircle.class) {
            for(int i=0; i<testObject.length; i++) {
                SpatiotemporalGeometry testGeometry = (SpatiotemporalGeometry) testObject[i];
                testGeometry.setUserData("This is a test for SpatiotemporalPoint");
                Object serdeGeometry = deserializeWithKryo(serializeWithKryo(testGeometry));
                Assert.assertEquals(testGeometry, serdeGeometry);
            }
        }

        System.out.println("\n==== Completed Correctness Test of " + aClass.toString() + " ====");
    }

    private void compareSize(int size, Class aClass) throws IOException {
        Object[] testObject = generateSpatiotemporalGeometry(size, aClass);
        List<Integer> noKryoLength = new ArrayList<>(size);
        List<Integer> withKryoLength = new ArrayList<>(size);
        List<Double> proportion = new ArrayList<>(size);

        for(int i=0; i<size; i++) {
            byte[] noKryo = serializeWithNOKryo(testObject[i]);
            byte[] withKryo = serializeWithKryo(testObject[i]);

            noKryoLength.add(noKryo.length);
            withKryoLength.add(withKryo.length);
            proportion.add(withKryo.length/(double) noKryo.length);
        }

        System.out.println("\n==== test Size of " + aClass.toString() + " ====");
        System.out.println("Original size: " + (int) noKryoLength.stream().mapToInt(val -> val).average().orElse(0));
        System.out.println("With Kryo size: " + (int) withKryoLength.stream().mapToInt(val -> val).average().orElse(0));
        System.out.println("Proportion: " + proportion.stream().mapToDouble(val -> val).average().orElse(0.0) * 100 + " %");
    }

    private void compareTime(int iteration, int size, Class aClass) throws IOException, ClassNotFoundException {
        System.out.println("\n==== test Serde time of " + aClass.toString() + " ====");
        Object[] testObject = generateSpatiotemporalGeometry(size, aClass);
        double before, after;
        List<Double> noKryoSerTime = new ArrayList<>(size);
        List<Double> noKryoDeTime = new ArrayList<>(size);
        List<Double> withKryoSerTime = new ArrayList<>(size);
        List<Double> withKryoDeTime = new ArrayList<>(size);

        for(int itr=0; itr<iteration; itr++) {
            for(int i=0; i<size; i++) {
                before = System.currentTimeMillis();
                byte[] noKryo = serializeWithNOKryo(testObject[i]);
                after = System.currentTimeMillis();
                noKryoSerTime.add(after - before);

                before = System.currentTimeMillis();
                deserializeWithNOKryo(noKryo);
                after = System.currentTimeMillis();
                noKryoDeTime.add(after - before);

                before = System.currentTimeMillis();
                byte[] withKryo = serializeWithKryo(testObject[i]);
                after = System.currentTimeMillis();
                withKryoSerTime.add(after - before);

                before = System.currentTimeMillis();
                deserializeWithKryo(withKryo);
                after = System.currentTimeMillis();
                withKryoDeTime.add(after - before);
            }
        }

        System.out.println("Data Size: " + size);
        System.out.println("Original Serialization time: " + noKryoSerTime.stream().mapToDouble(val -> val).sum()/iteration + " ms");
        System.out.println("Original Deserialization time: " + noKryoDeTime.stream().mapToDouble(val -> val).sum()/iteration + " ms");
        System.out.println("With Kryo Serialization time: " + withKryoSerTime.stream().mapToDouble(val -> val).sum()/iteration + " ms");
        System.out.println("With Kryo Deserialization time: " + withKryoDeTime.stream().mapToDouble(val -> val).sum()/iteration + " ms");
    }

    private Object[] generateSpatiotemporalGeometry(int size, Class aClass) {
        Random random = new Random();
        if(aClass == SpatiotemporalPoint.class) {
            SpatiotemporalPoint[] result = new SpatiotemporalPoint[size];
            for(int i=0; i<size; i++) {
                result[i] = new SpatiotemporalPoint(new Coordinate(random.nextDouble(), random.nextDouble()), LocalDateTime.now());
            }
            return result;
        } else if(aClass == SpatiotemporalCircle.class) {
            SpatiotemporalCircle[] result = new SpatiotemporalCircle[size];
            for(int i=0; i<size; i++) {
                SpatiotemporalPoint center = new SpatiotemporalPoint(new Coordinate(random.nextDouble(), random.nextDouble()), LocalDateTime.now());
                result[i] = new SpatiotemporalCircle(center, random.nextDouble(), 10, ChronoUnit.SECONDS);
            }
            return result;
        } else {
            SpatiotemporalEnvelope[] result = new SpatiotemporalEnvelope[size];
            for(int i=0; i<size; i++) {
                result[i] = new SpatiotemporalEnvelope(new Envelope(random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble()), LocalDateTime.now());
            }
            return result;
        }
    }

    private byte[] serializeWithKryo(Object inputObject) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        kryo.writeClassAndObject(output, inputObject);
        output.close();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private Object deserializeWithKryo(byte[] byteArray) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        Input input = new Input(inputStream);
        input.close();
        Object deserObject = kryo.readClassAndObject(input);
        inputStream.close();
        return deserObject;
    }

    private byte[] serializeWithNOKryo(Object inputObject) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutput output = new ObjectOutputStream(outputStream);
        output.writeObject(inputObject);
        output.close();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private Object deserializeWithNOKryo(byte[] byteArray) throws IOException, ClassNotFoundException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        ObjectInput input = new ObjectInputStream(inputStream);
        Object deserObject = input.readObject();
        input.close();
        inputStream.close();
        return deserObject;
    }

    public static String byteArrayToString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        sb.append("]");
        return sb.toString();
    }

    public void tempCode(Class aClass) throws IOException {
        //        SpatiotemporalPoint point = new SpatiotemporalPoint(new Coordinate(12484897.672639163, 3780484.2824323317),
//                LocalDateTime.parse("1999-11-03T00:00"));
//        SpatiotemporalCircle object = new SpatiotemporalCircle(point, 2000, 20, ChronoUnit.MONTHS);
        SpatiotemporalEnvelope object = new SpatiotemporalEnvelope(new Envelope(12186932.799134474, 12858928.490036784, 3407252.3486768603, 3648860.532667034),
                LocalDateTime.parse("1953-01-01T00:00"), LocalDateTime.parse("2004-10-15T00:00"));
        byte[] withKryo = serializeWithKryo(object);
        OutputStream withKryoOuput = new FileOutputStream(new File("D://withKryo-" + aClass.getSimpleName() + ".dat"));
        withKryoOuput.write(withKryo);
        withKryoOuput.close();

//        System.out.println(byteArrayToString(noKryo));
//        byte [] X = ByteBuffer.allocate(8).putDouble(((Point) object.getSpatialGeometry()).getX()).array();
//        byte [] Y = ByteBuffer.allocate(8).putDouble(((Point) object.getSpatialGeometry()).getY()).array();
//        byte [] time = ByteBuffer.allocate(8).putLong(TimeUtils.getTimestampFromDateTime(object.getStartTime())).array();
//        byte [] zoneId = object.getZoneId().toString().getBytes();
//        byte [] spatialRadius = ByteBuffer.allocate(8).putDouble(object.getSpatialRadius()).array();
//        byte [] temporalRadius = ByteBuffer.allocate(8).putLong(object.getTemporalRadius()).array();
//        byte [] temporalUnit = object.getTemporalUnit().toString().getBytes();
//        System.out.println(byteArrayToString(X));
//        System.out.println(byteArrayToString(Y));
//        System.out.println(byteArrayToString(time));
//        System.out.println(byteArrayToString(zoneId));
//        System.out.println(byteArrayToString(spatialRadius));
//        System.out.println(byteArrayToString(temporalRadius));
//        System.out.println(byteArrayToString(temporalUnit));

        byte [] minX = ByteBuffer.allocate(8).putDouble(object.spatialEnvelope.getMinX()).array();
        byte [] maxX = ByteBuffer.allocate(8).putDouble(object.spatialEnvelope.getMaxX()).array();
        byte [] minY = ByteBuffer.allocate(8).putDouble(object.spatialEnvelope.getMinY()).array();
        byte [] maxY = ByteBuffer.allocate(8).putDouble(object.spatialEnvelope.getMaxY()).array();
        byte [] startTime = ByteBuffer.allocate(8).putLong(TimeUtils.getTimestampFromDateTime(object.startTime)).array();
        byte [] endTime = ByteBuffer.allocate(8).putLong(TimeUtils.getTimestampFromDateTime(object.endTime)).array();
        byte [] zoneId = object.zoneId.toString().getBytes();
        byte [] id = ByteBuffer.allocate(4).putInt(object.id).array();

        System.out.println(byteArrayToString(minX));
        System.out.println(byteArrayToString(maxX));
        System.out.println(byteArrayToString(minY));
        System.out.println(byteArrayToString(maxY));
        System.out.println(byteArrayToString(startTime));
        System.out.println(byteArrayToString(endTime));
        System.out.println(byteArrayToString(zoneId));
        System.out.println(byteArrayToString(id));
    }
}
