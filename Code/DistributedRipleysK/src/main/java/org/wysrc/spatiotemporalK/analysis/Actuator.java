package org.wysrc.spatiotemporalK.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Coordinate;
import org.wysrc.spatiotemporalK.geom.SpatiotemporalPoint;
import org.wysrc.spatiotemporalK.geom.SpatiotemporalPolygon;
import org.wysrc.spatiotemporalK.index.IndexType;
import org.wysrc.spatiotemporalK.partitioner.PartitionerType;
import org.wysrc.spatiotemporalK.serde.RipleysKFunctionKryoRegistrator;
import org.wysrc.spatiotemporalK.spatiotemporalRDD.SpatiotemporalPointRDD;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

public class Actuator {

    public static void executeSpatiotemporalRipleysK(String master, String pointCSVPath, String polygonPath, boolean useEnvelopeBoundary,
                                              int pointSize, double maxSpatialDistance, long maxTemporalDistance,
                                              String temporalUnit, int spatialStep, int temporalStep,
                                              String simulationType, int simulationCount, int numPartitions, boolean usePartition,
                                              boolean useIndex, boolean useCache, boolean useKryo, String outputPath) throws Exception {
        SparkSession sparkSession;
        if (useKryo) {
            sparkSession = SparkSession.builder().
                    config("spark.serializer", KryoSerializer.class.getName()).
                    config("spark.kryo.registrator", RipleysKFunctionKryoRegistrator.class.getName()).
                    master(master).appName("Ripleys K Function").getOrCreate();
        } else {
            sparkSession = SparkSession.builder().master(master).appName("Ripleys K Function").getOrCreate();
        }

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        ChronoUnit temporalUnitEnum = null;
        for (ChronoUnit chronoUnit : ChronoUnit.values()) {
            if (chronoUnit.name().equalsIgnoreCase(temporalUnit)) {
                temporalUnitEnum = chronoUnit;
                break;
            }
        }
        if (temporalUnitEnum == null) {
            temporalUnitEnum = ChronoUnit.YEARS;
        }

        SimulationType simulationTypeEnum = null;
        for (SimulationType simulationTypeValue : SimulationType.values()) {
            if (simulationTypeValue.name().equalsIgnoreCase(simulationType)) {
                simulationTypeEnum = simulationTypeValue;
                break;
            }
        }
        if (simulationTypeEnum == null) {
            simulationTypeEnum = SimulationType.bootstrapping;
        }

        JavaRDD<SpatiotemporalPoint> pointJavaRDD = readPointByDataset(sparkSession, pointCSVPath, pointSize);

        // CRS Transform: (latitude, longitude) -> (x, y)
        SpatiotemporalPointRDD spatiotemporalPointRDD = new SpatiotemporalPointRDD(pointJavaRDD,
                "epsg:4326", "epsg:3857");

        spatiotemporalPointRDD.analyze();
        SpatiotemporalPolygon spatiotemporalBoundary = getSpatiotemporalBoundary(sparkSession, spatiotemporalPointRDD, polygonPath, useEnvelopeBoundary);

        RipleysKResult result = PointPatternAnalysis.SpatiotemporalRipleysKFunction(sparkSession, spatiotemporalPointRDD,
                maxSpatialDistance, maxTemporalDistance, temporalUnitEnum, spatiotemporalBoundary, spatialStep,
                temporalStep, EdgeCorrectionType.isotropic, simulationTypeEnum, simulationCount,
                PartitionerType.KDBTree, numPartitions, IndexType.STRTree, usePartition, useIndex, useCache);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        FSDataOutputStream outputStream = FileSystem.get(new Configuration()).create(new Path(outputPath));
        outputStream.write(result.toJsonString().getBytes(StandardCharsets.UTF_8));
        outputStream.close();

        sparkSession.stop();
    }

    public static JavaRDD<SpatiotemporalPoint> readPointByDataset(SparkSession sparkSession, String pointCSVPath, int size) {
        StructType spatiotemporalPointSchema = new StructType().
                add("latitude", "double").
                add("longitude", "double").
                add("time", "string");

        Dataset<Row> pointDS = sparkSession.read().format("csv").schema(spatiotemporalPointSchema).load(pointCSVPath).limit(size);

        JavaRDD<SpatiotemporalPoint> pointJavaRDD = pointDS.toJavaRDD().map(row -> {
            double x = row.getAs("latitude");
            double y = row.getAs("longitude");
            String time = "11/03/1999 00:00:00";
            DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("MM/dd/yyyy HH:mm:ss").
                    parseDefaulting(ChronoField.MONTH_OF_YEAR, 6).
                    parseDefaulting(ChronoField.DAY_OF_MONTH, 30).
                    parseDefaulting(ChronoField.HOUR_OF_DAY, 0).
                    parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).
                    parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter();
            LocalDateTime dateTime = LocalDateTime.parse(time.replace("\"", ""), formatter);
            return new SpatiotemporalPoint(new Coordinate(x, y), dateTime);
        });

        return pointJavaRDD;
    }

    public static SpatiotemporalPolygon getSpatiotemporalBoundary(SparkSession sparkSession, SpatiotemporalPointRDD spatiotemporalPointRDD, String polygonPath, boolean useEnvelopeBoundary) {
        if (useEnvelopeBoundary) {
            return new SpatiotemporalPolygon(spatiotemporalPointRDD.envelope);
        } else {
            Dataset<Row> polygonDS = sparkSession.read().format("json").option("multiline", "true").load(polygonPath);
            double[] coordinateValues = polygonDS.select(functions.explode(functions.col("geometries")).as("flattenGeometries"))
                    .select(functions.explode(functions.col("flattenGeometries.coordinates")).as("flattenCoordinates"))
                    .select(functions.explode(functions.col("flattenCoordinates")).as("tuples"))
                    .select(functions.explode(functions.col("tuples")).as("coordinateValues"))
                    .collectAsList().stream().mapToDouble(r -> r.getDouble(0)).toArray();
            return new SpatiotemporalPolygon(coordinateValues, spatiotemporalPointRDD.envelope.startTime,
                    spatiotemporalPointRDD.envelope.endTime, "epsg:4326", "epsg:3857", true);
        }
    }



}
