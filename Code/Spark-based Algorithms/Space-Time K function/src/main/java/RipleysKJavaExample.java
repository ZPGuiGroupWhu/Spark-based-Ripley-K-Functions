import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.whu.geoai_stval.spark_K_functions.space_time_K.analysis.EdgeCorrectionType;
import org.whu.geoai_stval.spark_K_functions.space_time_K.analysis.PointPatternAnalysis;
import org.whu.geoai_stval.spark_K_functions.space_time_K.analysis.RipleysKResult;
import org.whu.geoai_stval.spark_K_functions.space_time_K.analysis.SimulationType;
import org.whu.geoai_stval.spark_K_functions.space_time_K.geom.*;
import org.whu.geoai_stval.spark_K_functions.space_time_K.index.IndexType;
import org.whu.geoai_stval.spark_K_functions.space_time_K.index.kdbtree.KDBTree;
import org.whu.geoai_stval.spark_K_functions.space_time_K.index.strtree.STRTree;
import org.whu.geoai_stval.spark_K_functions.space_time_K.operator.JoinQuery;
import org.whu.geoai_stval.spark_K_functions.space_time_K.partitioner.PartitionerType;
import org.whu.geoai_stval.spark_K_functions.space_time_K.serde.RipleysKFunctionKryoRegistrator;
import org.whu.geoai_stval.spark_K_functions.space_time_K.spatiotemporalRDD.SpatiotemporalCircleRDD;
import org.whu.geoai_stval.spark_K_functions.space_time_K.spatiotemporalRDD.SpatiotemporalPointRDD;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class RipleysKJavaExample {
    public static void main(String[] args) throws Exception {
//        System.setProperty("hadoop.home.dir", "D:\\Development\\Hadoop");
//        String resourcePath = System.getProperty("user.dir")+"/src/test/resources/";
//        String pointCSVPath = resourcePath+"hubei.csv";
//        String polygonPath = resourcePath+"hubei.json";
//        String tripPath = resourcePath+"yellow_tripdata_2009-01-subset.csv";
        String master = args[0];
        String pointCSVPath = args[1];
        boolean useFormatter = Boolean.valueOf(args[2]);
        String polygonPath = args[3];
        boolean useEnvelopeBoundary = Boolean.valueOf(args[4]);
        int pointSize = Integer.valueOf(args[5]);
        double maxSpatialDistance = Double.valueOf(args[6]);
        long maxTemporalDistance = Long.valueOf(args[7]);
        String temporalUnit = args[8];
        int spatialStep = Integer.valueOf(args[9]);
        int temporalStep = Integer.valueOf(args[10]);
        int estimationCount = Integer.valueOf(args[11]);
        String simulationType = args[12];
        int simulationCount = Integer.valueOf(args[13]);
        int numPartitions = Integer.valueOf(args[14]);
        boolean usePartition = Boolean.valueOf(args[15]);
        boolean useIndex = Boolean.valueOf(args[16]);
        boolean useCache = Boolean.valueOf(args[17]);
        boolean useKryo = Boolean.valueOf(args[18]);
        String outputPath = args[19];

        executeRipleysK(master, pointCSVPath, useFormatter, polygonPath, useEnvelopeBoundary, pointSize, maxSpatialDistance, maxTemporalDistance, temporalUnit,
                spatialStep, temporalStep, numPartitions, estimationCount, simulationType, simulationCount, usePartition,
                useIndex, useCache, useKryo, outputPath);

//        System.out.println("Finished RipleysK Example.");
    }

    public static void executeRipleysK(String master, String pointCSVPath, boolean useFormatter, String polygonPath, boolean useEnvelopeBoundary,
                                       int pointSize, double maxSpatialDistance, long maxTemporalDistance,
                                       String temporalUnit, int spatialStep, int temporalStep, int numPartitions,
                                       int estimationCount, String simulationType, int simulationCount, boolean usePartition,
                                       boolean useIndex, boolean useCache, boolean useKryo, String outputPath) throws Exception {
        SparkSession sparkSession;
        if(useKryo) {
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
        for(ChronoUnit chronoUnit: ChronoUnit.values()) {
            if(chronoUnit.name().equalsIgnoreCase(temporalUnit)) {
                temporalUnitEnum = chronoUnit;
                break;
            }
        }
        if(temporalUnitEnum == null) {
            temporalUnitEnum = ChronoUnit.YEARS;
        }

        SimulationType simulationTypeEnum = null;
        for(SimulationType simulationTypeValue: SimulationType.values()) {
            if(simulationTypeValue.name().equalsIgnoreCase(simulationType)) {
                simulationTypeEnum = simulationTypeValue;
                break;
            }
        }
        if(simulationTypeEnum == null) {
            simulationTypeEnum = SimulationType.bootstrapping;
        }

        JavaRDD<SpatiotemporalPoint> pointJavaRDD = readPointByDataset(sparkSession, pointCSVPath, useFormatter, pointSize);

        // CRS Transform: (latitude, longitude) -> (x, y)
        SpatiotemporalPointRDD spatiotemporalPointRDD = new SpatiotemporalPointRDD(pointJavaRDD,
                "epsg:4326", "epsg:3857");

        /* Start timer */
//        Calendar startCalendar = Calendar.getInstance();
//        int startTime = (startCalendar.get(Calendar.HOUR_OF_DAY)*3600+startCalendar.get(Calendar.MINUTE)*60+
//                startCalendar.get(Calendar.SECOND))*1000 + startCalendar.get(Calendar.MILLISECOND);

//        double maxSpatialDistance = Math.min(spatiotemporalPointRDD.envelope.getWidth(), spatiotemporalPointRDD.envelope.getHeight()) / 4;
//        long maxTemporalDistance = (spatiotemporalPointRDD.envelope.getTemporalInterval(ChronoUnit.YEARS) + 3) / 4;

        spatiotemporalPointRDD.analyze();
        SpatiotemporalPolygon spatiotemporalBoundary = getSpatiotemporalBoundary(sparkSession, spatiotemporalPointRDD, polygonPath, useEnvelopeBoundary);

        RipleysKResult result = PointPatternAnalysis.SpatiotemporalRipleysKFunction(sparkSession, spatiotemporalPointRDD,
                maxSpatialDistance, maxTemporalDistance, temporalUnitEnum, spatiotemporalBoundary, spatialStep,
                temporalStep, EdgeCorrectionType.isotropic, PartitionerType.KDBTree, numPartitions, IndexType.STRTree,
                estimationCount, simulationTypeEnum, simulationCount, usePartition, useIndex, useCache);

//        System.out.println("\nKest: ");
//        result.getKest().forEach(i->System.out.println(i));
//        System.out.println("\nKmin: ");
//        result.getKmin().forEach(i->System.out.println(i));
//        System.out.println("\nKmax: ");
//        result.getKmax().forEach(i->System.out.println(i));
//        System.out.println("\nmaxSpatialDistance: " + result.getMaxSpatialDistance());

        /* Totally end timer */
//        Calendar endCalendar = Calendar.getInstance();
//        int endTime = (endCalendar.get(Calendar.HOUR_OF_DAY)*3600+endCalendar.get(Calendar.MINUTE)*60+
//                endCalendar.get(Calendar.SECOND))*1000 + endCalendar.get(Calendar.MILLISECOND);
//
//        System.out.println("Total time costed: " + (endTime - startTime) + " ms");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        FSDataOutputStream outputStream = FileSystem.get(new Configuration()).create(new Path(outputPath + "/" + pointSize + "-" + LocalDateTime.now().format(formatter) + ".json"));
        outputStream.write(result.toJsonString().getBytes(StandardCharsets.UTF_8));
        outputStream.close();

        sparkSession.stop();
    }

    // Method1: sparkcontext textFile
    public static JavaRDD<SpatiotemporalPoint> readPointByTextFile(SparkSession sparkSession, String pointCSVPath) {
        JavaRDD<SpatiotemporalPoint> pointJavaRDD = JavaSparkContext.fromSparkContext(sparkSession.sparkContext()).textFile(pointCSVPath).map(
            new Function<String, SpatiotemporalPoint>() {
                @Override
                public SpatiotemporalPoint call(String line) {
                    String[] fields = line.replace("\"","").split(",");
                    double x = Double.valueOf(fields[0]);
                    double y = Double.valueOf(fields[1]);
                    DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy").
                            parseDefaulting(ChronoField.MONTH_OF_YEAR, 6).
                            parseDefaulting(ChronoField.DAY_OF_MONTH, 30).
                            parseDefaulting(ChronoField.HOUR_OF_DAY, 0).
                            parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).
                            parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter();
                    LocalDateTime dateTime = LocalDateTime.parse(fields[2], formatter);
                    return new SpatiotemporalPoint(new Coordinate(x, y), dateTime);
                }
            }
        );

        pointJavaRDD.take(10).forEach(i->System.out.println(i.getSpatialGeometry()+" "+i.getStartTime()));
        return pointJavaRDD;
    }

    // Method2: Dataset Read
    public static JavaRDD<SpatiotemporalPoint> readPointByDataset(SparkSession sparkSession, String pointCSVPath, boolean useFormatter, int size) {
        StructType spatiotemporalPointSchema = new StructType().
                add("latitude", "double").
                add("longitude", "double").
                add("time", "string");

        Dataset<Row> pointDS = sparkSession.read().format("csv").schema(spatiotemporalPointSchema).load(pointCSVPath).limit(size);

        JavaRDD<SpatiotemporalPoint> pointJavaRDD = pointDS.toJavaRDD().map(row -> {
            double x = row.getAs("latitude");
            double y = row.getAs("longitude");
            String time = row.getAs("time");
            if(useFormatter) {
                DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("MM/dd/yyyy HH:mm:ss").
                        parseDefaulting(ChronoField.MONTH_OF_YEAR, 6).
                        parseDefaulting(ChronoField.DAY_OF_MONTH, 30).
                        parseDefaulting(ChronoField.HOUR_OF_DAY, 0).
                        parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).
                        parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter();
                LocalDateTime dateTime = LocalDateTime.parse(time.replace("\"",""), formatter);
                return new SpatiotemporalPoint(new Coordinate(x, y), dateTime);
            } else {
                LocalDateTime dateTime = LocalDateTime.parse(time);
                return new SpatiotemporalPoint(new Coordinate(x, y), dateTime);
            }
        });

        return pointJavaRDD;
    }

    public static SpatiotemporalPolygon getSpatiotemporalBoundary(SparkSession sparkSession, SpatiotemporalPointRDD spatiotemporalPointRDD, String polygonPath, boolean useEnvelopeBoundary) {
        if(useEnvelopeBoundary) {
            return new SpatiotemporalPolygon(spatiotemporalPointRDD.envelope);
        } else {
            Dataset<Row> polygonDS = sparkSession.read().format("json").option("multiline", "true").load(polygonPath);
            Dataset<Row> middle = polygonDS.select(functions.explode(functions.col("features")).as("flattenFeatures"));
            double[] coordinateValues = middle
                .select(functions.explode(functions.col("flattenFeatures.geometry.coordinates")).as("flattenCoordinates"))
                .select(functions.explode(functions.col("flattenCoordinates")).as("tuples"))
                .select(functions.explode(functions.col("tuples")).as("coordinateValues"))
                .collectAsList().stream().mapToDouble(r->r.getDouble(0)).toArray();
            return new SpatiotemporalPolygon(coordinateValues, spatiotemporalPointRDD.envelope.startTime,
                    spatiotemporalPointRDD.envelope.endTime, "epsg:4326", "epsg:3857", true);
        }
    }

//    public static void findMissingPair(SparkSession sparkSession, SpatiotemporalPointRDD spatiotemporalPointRDD) throws Exception {
//        spatiotemporalPointRDD.analyze();
//        double maxSpatialDistance = Math.max(spatiotemporalPointRDD.envelope.getWidth(), spatiotemporalPointRDD.envelope.getHeight()) / 4;
//        long maxTemporalDistance = (spatiotemporalPointRDD.envelope.getTemporalInterval(ChronoUnit.YEARS) + 3) / 4;
//        SpatiotemporalPolygon spatiotemporalBoundary = new SpatiotemporalPolygon(spatiotemporalPointRDD.envelope);
//
//        RipleysKResult result = PointPatternAnalysis.SpatiotemporalRipleysKFunction(sparkSession, spatiotemporalPointRDD,
//                maxSpatialDistance, maxTemporalDistance, ChronoUnit.YEARS, spatiotemporalBoundary, 1,
//                1, EdgeCorrectionType.isotropic, SimulationType.bootstrapping, 0,
//                PartitionerType.KDBTree, 70, IndexType.STRTree, false, false, false);
//
//        spatiotemporalPointRDD.rawRDD = spatiotemporalPointRDD.rawRDD.repartition(5);
//        PointPatternAnalysis.SpatiotemporalRipleysKFunction(sparkSession, spatiotemporalPointRDD,
//                maxSpatialDistance, maxTemporalDistance, ChronoUnit.YEARS, spatiotemporalBoundary, 1,
//                1, EdgeCorrectionType.isotropic, SimulationType.bootstrapping, 0,
//                PartitionerType.KDBTree, 50, IndexType.STRTree, false, false, false);
//        List<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>> beforeList = spatiotemporalPointRDD.beforePartitionPair.collect();
//        List<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>> afterList = spatiotemporalPointRDD.afterPartitionPair.collect();
//
//        System.out.println("beforePair count: " + beforeList.size());
//        System.out.println("afterPair count: " + afterList.size());
//        List<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>> missingList = new ArrayList<>();
//        for(int i=0; i<beforeList.size(); i++) {
//            Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> beforePair = beforeList.get(i);
//            boolean missing = true;
//            for(int j=0; j<afterList.size(); j++) {
//                Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> afterPair = afterList.get(j);
//                if(beforePair._1.equals(afterPair._1) && beforePair._2.equals(afterPair._2)) {
//                    missing = false;
//                }
//            }
//            if(missing) {
//                missingList.add(beforePair);
//            }
//        }
//        missingList.forEach(p->System.out.println(p));
//        System.out.println("missing count: " + missingList.size());
//        List<String> pointInfo = spatiotemporalPointRDD.rawRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<SpatiotemporalPoint>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer index, Iterator<SpatiotemporalPoint> spatiotemporalPointIterator) throws Exception {
//                List<String> result = new ArrayList<>();
//                while(spatiotemporalPointIterator.hasNext()) {
//                    SpatiotemporalPoint point = spatiotemporalPointIterator.next();
//                    for(Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> pair: missingList) {
//                        if(point.equals(pair._2)) {
//                            result.add("#"+index+" "+point.toString());
//                            break;
//                        }
//                    }
//                }
//                return result.iterator();
//            }
//        }, true).collect();
//        pointInfo.forEach(p->System.out.println(p));
//    }

    public static void localDemo(SparkSession sparkSession) throws Exception {
        /*------------------------- Local Variable Demo ------------------------------------*/
        List<SpatiotemporalPoint> pointList = new ArrayList<>();
        pointList.add(new SpatiotemporalPoint(new Coordinate(1, 2), LocalDateTime.now()));
        pointList.add(new SpatiotemporalPoint(new Coordinate(2, 6), LocalDateTime.now()));
        pointList.add(new SpatiotemporalPoint(new Coordinate(3, 4), LocalDateTime.now()));
        pointList.add(new SpatiotemporalPoint(new Coordinate(4, 1), LocalDateTime.now()));
        pointList.add(new SpatiotemporalPoint(new Coordinate(5, 3), LocalDateTime.now()));
        pointList.add(new SpatiotemporalPoint(new Coordinate(6, 5), LocalDateTime.now()));

        JavaRDD<SpatiotemporalPoint> tempPointRDD = JavaSparkContext.fromSparkContext(sparkSession.sparkContext()).parallelize(pointList);
        Function2 getPointRDDPartitionInfo = new Function2<Integer, Iterator<SpatiotemporalPoint>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<SpatiotemporalPoint> spatiotemporalPointIterator) {
                List<String> result = new ArrayList<>();
                while(spatiotemporalPointIterator.hasNext()) {
                    SpatiotemporalPoint point = spatiotemporalPointIterator.next();
                    result.add(index.toString()+"#"+point.toString());
                }
                return result.iterator();
            }
        };
        Function2 getCirclePartitionInfo = new Function2<Integer, Iterator<SpatiotemporalCircle>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<SpatiotemporalCircle> spatiotemporalCircleIterator) {
                List<String> result = new ArrayList<>();
                while(spatiotemporalCircleIterator.hasNext()) {
                    SpatiotemporalCircle circle = spatiotemporalCircleIterator.next();
                    result.add(index.toString()+"#"+circle.toString());
                }
                return result.iterator();
            }
        };

        SpatiotemporalPointRDD spatiotemporalPointRDD = new SpatiotemporalPointRDD(tempPointRDD);
        System.out.println("numPartitions of rawPointRDD: " + spatiotemporalPointRDD.rawRDD.getNumPartitions());
        spatiotemporalPointRDD.rawRDD.mapPartitionsWithIndex(getPointRDDPartitionInfo, true).foreach(i->System.out.println(i));

        for(int r=0; r<10; r++) {
            JavaRDD<SpatiotemporalPoint> sampledRawRDD = spatiotemporalPointRDD.rawRDD.sample(true, 1.0);
            SpatiotemporalPointRDD sampledPointRDD = new SpatiotemporalPointRDD(sampledRawRDD);
            System.out.println("\nSimulation " + r + ":");
            System.out.println("number of samples: " + sampledRawRDD.count());
            sampledPointRDD.rawRDD.mapPartitionsWithIndex(getPointRDDPartitionInfo, true).foreach(i->System.out.println(i));
        }

        spatiotemporalPointRDD.spatiotemporalPartitioning(PartitionerType.KDBTree, 2);
        System.out.println("numPartitions of partitionedPointRDD: " + spatiotemporalPointRDD.partitionedRDD.getNumPartitions());
        spatiotemporalPointRDD.partitionedRDD.mapPartitionsWithIndex(getPointRDDPartitionInfo, true).foreach(i->System.out.println(i));

        SpatiotemporalCircleRDD spatiotemporalCircleRDD = new SpatiotemporalCircleRDD(spatiotemporalPointRDD,
                3, 1, ChronoUnit.SECONDS);
        spatiotemporalCircleRDD.spatiotemporalPartitioning(spatiotemporalPointRDD.getPartitioner());
        System.out.println("numPartitions of rawCircleRDD: " + spatiotemporalCircleRDD.rawRDD.getNumPartitions());
        spatiotemporalCircleRDD.rawRDD.mapPartitionsWithIndex(getCirclePartitionInfo, true).foreach(i->System.out.println(i));
        System.out.println("numPartitions of partitionedCircleRDD: " + spatiotemporalCircleRDD.partitionedRDD.getNumPartitions());
        spatiotemporalCircleRDD.partitionedRDD.mapPartitionsWithIndex(getCirclePartitionInfo, true).foreach(i->System.out.println(i));

        spatiotemporalPointRDD.buildIndex(IndexType.STRTree, true);
        spatiotemporalPointRDD.indexedPartitionedRDD.foreach(i->System.out.println(i.toString()));

        JavaPairRDD<SpatiotemporalGeometry, SpatiotemporalPoint> pairRDD = JoinQuery.FlatDistanceJoinQuery(spatiotemporalCircleRDD,
                spatiotemporalPointRDD, true, true, true);
        pairRDD.foreach(i->System.out.println(i.toString()));
        System.out.println("Pair Count: "+pairRDD.count());

        JavaPairRDD<SpatiotemporalPoint, Double> localK = pairRDD.mapToPair(
                pair ->
                {
                    Double weight = 1d;
                    return new Tuple2<>((SpatiotemporalPoint) pair._1(), weight);
                }
        );
        localK.foreach(i->System.out.println(i.toString()));
        Double globalK = localK.reduceByKey((a, b) -> a+b).values().reduce((a, b) -> a+b);
        System.out.println("GlobalK: "+globalK);
        /*------------------------- Local Variable Demo -------------END-----------------*/
    }

//    private static void hashMapDemo(SparkSession sparkSession) {
//        List<SpatiotemporalPoint> pointList = new ArrayList<>();
//        pointList.add(new SpatiotemporalPoint(new Coordinate(1, 9), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(2, 6), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(3, 4), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(4, 10), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(5, 11), LocalDateTime.parse("2019-02-28T06:30:00")));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(6, 5), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(7, 3), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(8, 12), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(9, 2), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(10, 1), LocalDateTime.now()));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(11, 7), LocalDateTime.parse("2019-02-28T08:30:00")));
//        pointList.add(new SpatiotemporalPoint(new Coordinate(12, 8), LocalDateTime.now()));
//
//        JavaRDD<SpatiotemporalPoint> tempPointRDD = JavaSparkContext.fromSparkContext(sparkSession.sparkContext()).parallelize(pointList).repartition(2);
//        SpatiotemporalPointRDD spatiotemporalPointJavaRDD = new SpatiotemporalPointRDD(tempPointRDD);
//
//        JavaRDD<HashMap<Point, HashMap<Point, Double>>> spatialWeightMapRDD = null;
//        JavaRDD<HashMap<LocalDateTime, HashMap<LocalDateTime, Double>>> temporalWeightMapRDD = null;
//
//        spatialWeightMapRDD = spatiotemporalPointJavaRDD.rawRDD.mapPartitions(spatiotemporalPointIterator -> {
//            List<HashMap<Point, HashMap<Point, Double>>> result = new ArrayList<>();
//            HashMap<Point, HashMap<Point, Double>> weightMapOfPartition = new HashMap<>();
//            while(spatiotemporalPointIterator.hasNext()) {
//                SpatiotemporalPoint spatiotemporalPoint = spatiotemporalPointIterator.next();
//                weightMapOfPartition.put((Point) spatiotemporalPoint.getSpatialGeometry(), new HashMap<>());
//            }
//            result.add(weightMapOfPartition);
//            return result.iterator();
//        });
//
//        temporalWeightMapRDD = spatiotemporalPointJavaRDD.rawRDD.mapPartitions(spatiotemporalPointIterator -> {
//            List<HashMap<LocalDateTime, HashMap<LocalDateTime, Double>>> result = new ArrayList<>();
//            HashMap<LocalDateTime, HashMap<LocalDateTime, Double>> weightMapOfPartition = new HashMap<>();
//            while(spatiotemporalPointIterator.hasNext()) {
//                SpatiotemporalPoint spatiotemporalPoint = spatiotemporalPointIterator.next();
//                weightMapOfPartition.put(spatiotemporalPoint.getStartTime(), new HashMap<>());
//            }
//            result.add(weightMapOfPartition);
//            return result.iterator();
//        });
//
//        int[] partitionIds = new int[]{0, 1};
//        double spatialWeight = 1.1;
//        double temporalWeight = 1.2;
////        SpatiotemporalPoint centerPoint = new SpatiotemporalPoint(new Coordinate(5, 11), LocalDateTime.parse("2019-02-28T06:30:00"));
////        SpatiotemporalPoint neighborPoint = new SpatiotemporalPoint(new Coordinate(11, 7), LocalDateTime.parse("2019-02-28T08:30:00"));
//
//        SpatiotemporalPoint neighborPoint = new SpatiotemporalPoint(new Coordinate(5, 11), LocalDateTime.parse("2019-02-28T06:30:00"));
//        SpatiotemporalPoint centerPoint = new SpatiotemporalPoint(new Coordinate(11, 7), LocalDateTime.parse("2019-02-28T08:30:00"));
//
//        spatialWeightMapRDD = spatialWeightMapRDD.mapPartitionsWithIndex((index, mapIterator) -> {
//            for(int i=0; i<partitionIds.length; i++) {
//                if(partitionIds[i] == index) {
//                    List<HashMap<Point, HashMap<Point, Double>>> result = new ArrayList<>();
//                    while(mapIterator.hasNext()) {
//                        HashMap<Point, HashMap<Point, Double>> outerMap = mapIterator.next();
//                        HashMap<Point, Double> innerMap = outerMap.get((Point) centerPoint.getSpatialGeometry());
//                        if(innerMap != null) {
//                            innerMap.put((Point) neighborPoint.getSpatialGeometry(), spatialWeight);
//                        }
//                        result.add(outerMap);
//                    }
//                    return result.iterator();
//                }
//            }
//            return mapIterator;
//        }, true);
//
//        temporalWeightMapRDD = temporalWeightMapRDD.mapPartitionsWithIndex((index, mapIterator) -> {
//            for(int i=0; i<partitionIds.length; i++) {
//                if (partitionIds[i] == index) {
//                    List<HashMap<LocalDateTime, HashMap<LocalDateTime, Double>>> result = new ArrayList<>();
//                    while (mapIterator.hasNext()) {
//                        HashMap<LocalDateTime, HashMap<LocalDateTime, Double>> outerMap = mapIterator.next();
//                        HashMap<LocalDateTime, Double> innerMap = outerMap.get(centerPoint.getStartTime());
//                        if(innerMap != null) {
//                            innerMap.put(neighborPoint.getStartTime(), temporalWeight);
//                        }
//                        result.add(outerMap);
//                    }
//                    return result.iterator();
//                }
//            }
//            return mapIterator;
//        }, true);
//
//        List<HashMap<Point, HashMap<Point, Double>>>[] spatialWeightMaps = spatialWeightMapRDD.collectPartitions(partitionIds);
//        List<HashMap<LocalDateTime, HashMap<LocalDateTime, Double>>>[] temporalWeightMaps = temporalWeightMapRDD.collectPartitions(partitionIds);
//        Double existingSpatialWeight = null;
//        Double existingTemporalWeight = null;
//        for(int i=0; i<partitionIds.length; i++) {
//            HashMap<Point, Double> innerSpatialWeightMap = spatialWeightMaps[i].get(0).get((Point) centerPoint.getSpatialGeometry());
//            HashMap<LocalDateTime, Double> innerTemporalWeightMap = temporalWeightMaps[i].get(0).get(centerPoint.getStartTime());
//            if(innerSpatialWeightMap == null || innerTemporalWeightMap == null) {
//                continue;
//            }
//            existingSpatialWeight = innerSpatialWeightMap.get((Point) neighborPoint.getSpatialGeometry());
//            existingTemporalWeight = innerTemporalWeightMap.get(neighborPoint.getStartTime());
//        }
//        System.out.println("weight: "+existingSpatialWeight*existingTemporalWeight);
//    }

    public static void testKDBTree(SpatiotemporalPointRDD spatiotemporalPointRDD) {
        SpatiotemporalEnvelope envelope = new SpatiotemporalEnvelope(new Envelope(12069923.212569304, 12925921.672791082, 3384738.580637889, 3926581.2398705394),
                LocalDateTime.parse("1920-10-01T00:00"), LocalDateTime.parse("2019-12-31T00:00"));
        List<SpatiotemporalPoint> points = spatiotemporalPointRDD.rawRDD.collect();
        KDBTree kdbTree = new KDBTree(500, 1000, envelope);
        for(SpatiotemporalPoint point: points) {
            kdbTree.insert(point.getEnvelopeInternal(), point);
        }
        kdbTree.assignLeafEnvelopeIds();
        System.out.println("number of envelopes: "+kdbTree.getIdBase());
        System.out.println("number of items: "+kdbTree.getNumItems());
    }

    public static void testSTRTree(SpatiotemporalPointRDD spatiotemporalPointRDD) {
        List<SpatiotemporalPoint> points = spatiotemporalPointRDD.rawRDD.collect();
        List<SpatiotemporalCircle> circles = new SpatiotemporalCircleRDD(spatiotemporalPointRDD, 6000, 2, ChronoUnit.YEARS).rawRDD.collect();
        List<Pair<SpatiotemporalCircle, SpatiotemporalPoint>> result1 = new ArrayList<>();
        List<Pair<SpatiotemporalCircle, SpatiotemporalPoint>> result2 = new ArrayList<>();

        /* Start timer */
        Calendar startCalendar = Calendar.getInstance();
        int startTime = (startCalendar.get(Calendar.HOUR_OF_DAY)*3600+startCalendar.get(Calendar.MINUTE)*60+
                startCalendar.get(Calendar.SECOND))*1000 + startCalendar.get(Calendar.MILLISECOND);

        STRTree strTree = new STRTree(30, null);
        for(SpatiotemporalPoint point: points) {
            strTree.insert(point.getEnvelopeInternal(), point);
        }
        strTree.build();

        /* Index build end timer */
        Calendar buildIndexCalendar = Calendar.getInstance();
        int buildIndexTime = (buildIndexCalendar.get(Calendar.HOUR_OF_DAY)*3600+buildIndexCalendar.get(Calendar.MINUTE)*60+
                buildIndexCalendar.get(Calendar.SECOND))*1000 + buildIndexCalendar.get(Calendar.MILLISECOND);

        System.out.println("Build Index costed: " + (buildIndexTime - startTime) + " ms");

//        Random random = new Random();

        int candidateTime = 0;
        for(SpatiotemporalCircle circle: circles) {
            List<SpatiotemporalPoint> candidates = strTree.query(circle.getEnvelopeInternal());

            Calendar candidateStartCalendar = Calendar.getInstance();
            int candidateStartTime = (candidateStartCalendar.get(Calendar.HOUR_OF_DAY)*3600+candidateStartCalendar.get(Calendar.MINUTE)*60+
                    candidateStartCalendar.get(Calendar.SECOND))*1000 + candidateStartCalendar.get(Calendar.MILLISECOND);

            for(SpatiotemporalPoint candidate: candidates) {
                if(circle.intersects(candidate)) {
                    result1.add(Pair.of(circle, candidate));
                }
            }

            Calendar candidateEndCalendar = Calendar.getInstance();
            int candidateEndtTime = (candidateEndCalendar.get(Calendar.HOUR_OF_DAY)*3600+candidateEndCalendar.get(Calendar.MINUTE)*60+
                    candidateEndCalendar.get(Calendar.SECOND))*1000 + candidateEndCalendar.get(Calendar.MILLISECOND);
            candidateTime += (candidateEndtTime - candidateStartTime);
        }

        System.out.println("Query candidate costed: " + candidateTime + " ms");

//        for(int i=0; i<100; i++) {
//            SpatiotemporalCircle randomCircle = circles.get(random.nextInt(circles.size()));
//            List<SpatiotemporalPoint> candidates = strTree.query(randomCircle.getEnvelopeInternal());
//            for(SpatiotemporalPoint candidate: candidates) {
//                if(randomCircle.intersects(candidate)) {
//                    result1.add(Pair.of(randomCircle, candidate));
//                }
//            }
//        }

        /* Index query end timer */
        Calendar queryIndexCalendar = Calendar.getInstance();
        int queryIndexTime = (queryIndexCalendar.get(Calendar.HOUR_OF_DAY)*3600+queryIndexCalendar.get(Calendar.MINUTE)*60+
                queryIndexCalendar.get(Calendar.SECOND))*1000 + queryIndexCalendar.get(Calendar.MILLISECOND);

        System.out.println("Query Index costed: " + (queryIndexTime - buildIndexTime) + " ms");

        for(SpatiotemporalCircle circle: circles) {
            for(SpatiotemporalPoint point: points) {
                if(circle.intersects(point)) {
                    result2.add(Pair.of(circle, point));
                }
            }
        }

//        for(int i=0; i<100; i++) {
//            SpatiotemporalCircle randomCircle = circles.get(random.nextInt(circles.size()));
//            for(SpatiotemporalPoint point: points) {
//                if(randomCircle.intersects(point)) {
//                    result2.add(Pair.of(randomCircle, point));
//                }
//            }
//        }

        /* Loop end timer */
        Calendar loopCalendar = Calendar.getInstance();
        int loopTime = (loopCalendar.get(Calendar.HOUR_OF_DAY)*3600+loopCalendar.get(Calendar.MINUTE)*60+
                loopCalendar.get(Calendar.SECOND))*1000 + loopCalendar.get(Calendar.MILLISECOND);

        System.out.println("Loop costed: " + (loopTime - queryIndexTime) + " ms");
    }
}
