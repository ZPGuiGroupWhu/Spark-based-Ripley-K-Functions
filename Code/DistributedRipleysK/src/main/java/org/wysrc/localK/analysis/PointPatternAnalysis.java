package org.wysrc.localK.analysis;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.wysrc.localK.geom.SpatiotemporalGeometry;
import org.wysrc.localK.geom.SpatiotemporalPoint;
import org.wysrc.localK.geom.SpatiotemporalPolygon;
import org.wysrc.localK.index.IndexType;
import org.wysrc.localK.operator.JoinQuery;
import org.wysrc.localK.partitioner.PartitionerType;
import org.wysrc.localK.partitioner.SpatiotemporalPartitioner;
import org.wysrc.localK.spatiotemporalRDD.SpatiotemporalCircleRDD;
import org.wysrc.localK.spatiotemporalRDD.SpatiotemporalPointRDD;
import scala.Tuple2;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.*;

public class PointPatternAnalysis {
    private static final Logger log = LogManager.getLogger(PointPatternAnalysis.class);
    private static StructType spatiotemporalPointSchema = new StructType().
            add("x", "double").
            add("y", "double").
            add("time", "string");
    private static Dataset<Row> observedPointDF = null;
    private static JavaRDD<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>> spatiotemporalWeightMapRDD;
    private static double spatialLocationTolerance = 1;
    private static long spatialDistanceTolerance = 100;
    private static GeometryFactory geometryFactory = new GeometryFactory();

    public static RipleysKResult SpatiotemporalRipleysKFunction(
            SparkSession sparkSession,
            SpatiotemporalPointRDD spatiotemporalPointRDD,
            double maxSpatialDistance,
            long maxTemporalDistance,
            TemporalUnit temporalUnit,
            SpatiotemporalPolygon spatiotemporalBoundary,
            int spatialStep,
            int temporalStep,
            EdgeCorrectionType edgeCorrectionType,
            SimulationType simulationType,
            int simulationCount,
            PartitionerType partitionerType,
            int numPartitions,
            IndexType indexType,
            boolean usePartition,
            boolean useIndex,
            boolean useCache) throws Exception {
        if(maxTemporalDistance % temporalStep != 0) {
            throw new IllegalArgumentException("[SpatiotemporalRipleysKFunction] maxTemporalDistance must be divisible by temporalStep, please check your input.");
        }

        /* Ripley's K Estimation */
        // execute possible partitioning and indexing
        organizeSpatiotemporalPointRDD(sparkSession, spatiotemporalPointRDD, partitionerType, numPartitions, indexType,
                null, usePartition, useIndex, useCache, true);
        // get K value of observed point pattern
        List Kest = SpatiotemporalRipleysKFunction4SinglePointPattern(spatiotemporalPointRDD, maxSpatialDistance,
                maxTemporalDistance, temporalUnit, spatiotemporalBoundary, spatialStep, temporalStep, edgeCorrectionType,
                usePartition, useIndex, useCache, true);

        /* Ripley's K Simulation */
        // prepare for result structure
        HashMap<Point, Tuple2<Double[],Double[]>> mp = new HashMap<>();
//        for ()
        for (int j = 0 ;j < Kest.size(); ++j) {
            Tuple2<Point, Double[]> val = (Tuple2<Point, Double[]>) (Kest.get(j));
            Double[] min = new Double[val._2.length];
            Double[] max = new Double[val._2.length];
            for (int i = 0; i < val._2.length; ++i) {
                min[i] = max[i] = val._2[i];
            }
            mp.put(val._1, new Tuple2<>(min, max));
        }
        List<PointResult> ansEst = new ArrayList<>();
        List<PointResult> ansMin = new ArrayList<>();
        List<PointResult> ansMax = new ArrayList<>();



        // get K value of simulated point pattern
        for(int s=0; s<simulationCount; s++) {
            // generate simulated points
            SpatiotemporalPointRDD simulatedPointRDD = simulateSpatiotemporalPoint(sparkSession,
                    (int) spatiotemporalPointRDD.totalCount, simulationType);
            //System.out.println("simulated" + s);
            //System.out.println(simulatedPointRDD.totalCount);

            simulatedPointRDD.totalCount = spatiotemporalPointRDD.totalCount;

            // organize the simulated points
            organizeSpatiotemporalPointRDD(sparkSession, simulatedPointRDD, partitionerType, numPartitions, indexType,
                    spatiotemporalPointRDD.getPartitioner(), usePartition, useIndex, useCache, false);
            // execute K function on simulated points
            List Ksim = SpatiotemporalRipleysKFunction4SinglePointPattern(simulatedPointRDD, maxSpatialDistance,
                    maxTemporalDistance, temporalUnit, spatiotemporalBoundary, spatialStep, temporalStep, edgeCorrectionType,
                    usePartition, useIndex, useCache, false);
            for (int i = 0; i < Ksim.size(); ++i) {
                Tuple2<Point, Double[]> val = (Tuple2<Point, Double[]>) (Ksim.get(i));
                Point key = val._1;
                if (mp.get(key) == null) {
                    Double[] Kmin = new Double[spatialStep + 1];
                    Double[] Kmax = new Double[spatialStep + 1];
                    for(int j=0; j<spatialStep+1; j++) {
                        if(j == 0) {
                            Kmax[j]=0d;
                            Kmin[j]=0d;
                            continue;
                        }
                        Kmin[j] = Double.MAX_VALUE;
                        Kmax[j] = Double.MIN_VALUE;
                    }
                    mp.put(key, new Tuple2<>(Kmin, Kmax));
                }
                for(int j=0; j<spatialStep+1; j++) {

                    double KsimValue = val._2[j];
                    double KminValue = mp.get(key)._1[j];
                    double KmaxValue = mp.get(key)._2[j];
                    if(KsimValue < KminValue) {
//                        Kmin.set(j, KsimValue);
                        mp.get(key)._1[j] = KsimValue;
                    }
                    if(KsimValue > KmaxValue) {
//                        Kmax.set(j, KsimValue);
                        mp.get(key)._2[j] = KsimValue;
                    }
                }
            }
        }

        for (int j = 0 ;j < Kest.size(); ++j) {
            Tuple2<Point, Double[]> val = (Tuple2<Point, Double[]>) (Kest.get(j));

            CoordinateReferenceSystem sourceCRS = CRS.decode("epsg:3857");
            CoordinateReferenceSystem targetCRS = CRS.decode("epsg:4326");
            final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
            Point point = (Point) JTS.transform(val._1, transform);
            double x = point.getX();
            double y = point.getY();
            ansEst.add(new PointResult(x, y, val._2));
        }

        for (int i = 0; i < Kest.size(); ++i) {
            Tuple2<Point, Double[]> item = (Tuple2<Point, Double[]>) (Kest.get(i));
            Point key = item._1;

            CoordinateReferenceSystem sourceCRS = CRS.decode("epsg:3857");
            CoordinateReferenceSystem targetCRS = CRS.decode("epsg:4326");
            final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
            Point point = (Point) JTS.transform(key, transform);
            double x = point.getX();
            double y = point.getY();


            Tuple2<Double[], Double[]> val = (Tuple2<Double[], Double[]>) (mp.get(key));
            ansMin.add(new PointResult(x, y, val._1));
            ansMax.add(new PointResult(x, y, val._2));
        }
        if (ansEst.size() != ansMax.size() || ansEst.size() != ansMin.size()) {
            System.out.println("size error");
            System.out.println("ansMax: "+ansMax.size());
            System.out.println("ansMin: "+ansMin.size());
            System.out.println("ansEst: "+ansEst.size());
        }
        return new RipleysKResult(ansEst, ansMin, ansMax, maxSpatialDistance, maxTemporalDistance, temporalUnit);
    }

    private static List SpatiotemporalRipleysKFunction4SinglePointPattern(
            SpatiotemporalPointRDD spatiotemporalPointRDD,
            double maxSpatialDistance,
            long maxTemporalDistance,
            TemporalUnit temporalUnit,
            SpatiotemporalPolygon spatiotemporalBoundary,
            int spatialStep,
            int temporalStep,
            EdgeCorrectionType edgeCorrectionType,
            boolean usePartition,
            boolean useIndex,
            boolean useCache,
            boolean isEstimation) throws Exception {
        /* check total count and envelope, compute intensity */
        if(spatiotemporalPointRDD.totalCount == -1) {
            spatiotemporalPointRDD.analyze();
        }
        double intensity = spatiotemporalBoundary.getVolume(temporalUnit) == 0? spatiotemporalPointRDD.totalCount:
                (spatiotemporalPointRDD.totalCount / spatiotemporalBoundary.getVolume(temporalUnit));

        /* find qualified point pairs at current spatial and temporal distance*/
        JavaPairRDD<SpatiotemporalGeometry, SpatiotemporalPoint> pairRDD = findQualifiedPairs(spatiotemporalPointRDD,
                maxSpatialDistance, maxTemporalDistance, temporalUnit, usePartition, useIndex);

        /* get K value for each Point Pair at multiple distances */
        JavaRDD<Tuple2<Point,Double[]>> KValuesRDD;
        if(useCache) {
            if(isEstimation) {
                buildSpatiotemporalWeightMap(pairRDD, maxSpatialDistance, maxTemporalDistance,
                        temporalUnit, spatialStep, temporalStep, spatiotemporalBoundary, edgeCorrectionType);
//                KValuesRDD = computeKAndBuildSpatiotemporalWeightMap(pairRDD, maxSpatialDistance, maxTemporalDistance,
//                        temporalUnit, spatialStep, temporalStep, spatiotemporalBoundary, edgeCorrectionType, intensity);
            }
            // after the partition, the center point and the points whose distance is below max is in this partition
            // so use hash to calculate in each partition, no need to merge in different partition
            KValuesRDD = pairRDD.zipPartitions(spatiotemporalWeightMapRDD, new FlatMapFunction2<Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>>, Iterator<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>>, Tuple2<Point,Double[]>>() {
                @Override
                public Iterator<Tuple2<Point,Double[]>> call(Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>> pointPairIterator, Iterator<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>> mapIterator) throws Exception {
                    List<Tuple2<Point,Double[]>> result = new ArrayList<>();
                    HashMap<Point, Double[]> mp = new HashMap<>();
                    if(!pointPairIterator.hasNext() || !mapIterator.hasNext()) {
                        return result.iterator();
                    }
                    Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>> partitionWeightMap = mapIterator.next();
                    // iterate all pairs
                    while(pointPairIterator.hasNext()) {
                        Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> pointPair = pointPairIterator.next();
                        Point centerPoint = (Point) pointPair._1.getSpatialGeometry();
                        Point neighborPoint = (Point) pointPair._2.getSpatialGeometry();

                        double pairSpatialDistance = calSpatialDistance(centerPoint, neighborPoint);
                        //long pairTemporalDistance = calTemporalDistance(centerTime, neighborTime, temporalUnit);

                        Point roundCenterPoint = geometryFactory.createPoint(new Coordinate(Math.round(centerPoint.getX()/spatialLocationTolerance)*spatialLocationTolerance,
                                Math.round(centerPoint.getY()/spatialLocationTolerance)*spatialLocationTolerance));
                        long roundSpatialDistance = Math.round(pairSpatialDistance/spatialDistanceTolerance)*spatialDistanceTolerance;

                        // get spatial weight
                        double spatialWeight;
                        if(!partitionWeightMap._1.containsKey(roundCenterPoint) || !partitionWeightMap._1.get(roundCenterPoint).containsKey(roundSpatialDistance)) {
                            spatialWeight = computeSpatialWeight(centerPoint, neighborPoint,
                                    (Polygon) spatiotemporalBoundary.getSpatialGeometry(), edgeCorrectionType);
                        } else {
                            spatialWeight = partitionWeightMap._1.get(roundCenterPoint).get(roundSpatialDistance);
                        }


                        double spatiotemporalWeight = spatialWeight;
                        // record in the hash map, key is center point
                        Double[] partitionKValues;
                        if (mp.containsKey(centerPoint)) {
                            partitionKValues = mp.get(centerPoint);
                        } else {
                            partitionKValues = new Double[spatialStep + 1];
                            Arrays.fill(partitionKValues, 0d);
                        }
                        for(int i = spatialStep; i>0; i--) {
                            double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
                            if(pairSpatialDistance > currentSpatialDistance) {
                                break;
                            }
                            // handle this pair, and count the influence of neighborPoint
                            partitionKValues[i] += spatiotemporalWeight;
                        }
                        mp.put(centerPoint, partitionKValues);
                    }
                    for (Point pt: mp.keySet()) {
                        Double[] partitionKValues = mp.get(pt);
                        for(int i=1; i<spatialStep + 1; i++) {
                            Double originalKValue = partitionKValues[i];
                            Double finalKValue = (originalKValue) / (intensity);
                            partitionKValues[i] = Math.sqrt(finalKValue / (Math.PI)) - (maxSpatialDistance / spatialStep) * i;
                        }
                        result.add(new Tuple2(pt,mp.get(pt)));
                    }
                    return result.iterator(); // this is ans, Tuple2 (CenterPoint, double[]), no reduce
                }
            });

        } else {
            KValuesRDD = pairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>>, Tuple2<Point,Double[]>>() {
                @Override
                public Iterator<Tuple2<Point,Double[]>> call(Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>> pointPairIterator) throws Exception {
                    List<Tuple2<Point,Double[]>> result = new ArrayList<>();
                    HashMap<Point,Double[]> mp = new HashMap<>();
                    if(!pointPairIterator.hasNext()) {
                        return result.iterator();
                    }
                    while(pointPairIterator.hasNext()) {
                        Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> pointPair = pointPairIterator.next();
                        double spatiotemporalWeight = computeWeight((SpatiotemporalPoint) pointPair._1(), pointPair._2(),
                                spatiotemporalBoundary, edgeCorrectionType);
                        double pairSpatialDistance = calSpatialDistance((Point) pointPair._1().getSpatialGeometry(),
                                (Point) pointPair._2().getSpatialGeometry());
                        Point centerPoint = (Point) pointPair._1.getSpatialGeometry();
                        Double[] partitionKValues;
                        if (mp.containsKey(centerPoint)) {
                            partitionKValues = mp.get(centerPoint);
                        } else {
                            partitionKValues = new Double[spatialStep + 1];
                            Arrays.fill(partitionKValues, 0d);
                        }
                        for(int i=spatialStep; i>0; i--) {
                            double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
                            if(pairSpatialDistance > currentSpatialDistance) {
                                break;
                            }
                            partitionKValues[i] += spatiotemporalWeight;
                        }
                        mp.put(centerPoint, partitionKValues);
                    }

                    for (Point pt: mp.keySet()) {
                        Double[] partitionKValues = mp.get(pt);
                        for(int i=1; i<spatialStep + 1; i++) {
                            Double originalKValue = partitionKValues[i];
                            Double finalKValue = originalKValue / (intensity);
                            partitionKValues[i] = Math.sqrt(finalKValue / (Math.PI)) - (maxSpatialDistance / spatialStep) * i;
                        }
                        result.add(new Tuple2(pt,mp.get(pt)));
                    }
                    return result.iterator();
                }
            }, true);
        }

        List<Tuple2<Point,Double[]>> finalResult = KValuesRDD.collect();
        return finalResult;
    }

    private static void organizeSpatiotemporalPointRDD(SparkSession sparkSession, SpatiotemporalPointRDD spatiotemporalPointRDD,
                                                       PartitionerType partitionerType, int numPartitions,
                                                       IndexType indexType, SpatiotemporalPartitioner estimationPartitioner,
                                                       boolean usePartition, boolean useIndex, boolean useCache, boolean isEstimation) throws Exception {
        if(usePartition) {
            if(isEstimation) {
                //观测模式时该项为False，也就是这个选项只执行一次，得到一个分析模式，计算一个时空权重出来，然后后续
                //观测点均复用该分区形式。
                spatiotemporalPointRDD.spatiotemporalPartitioning(partitionerType, numPartitions);
                initObservedPointDF(sparkSession, spatiotemporalPointRDD.partitionedRDD);
            } else {
                if(useCache) {
                    //System.out.println("观测模式：使用缓存！");
                    spatiotemporalPointRDD.spatiotemporalPartitioning(estimationPartitioner);
                } else {
                    //System.out.println("观测模式：未使用缓存，重新分区！");
                    spatiotemporalPointRDD.spatiotemporalPartitioning(partitionerType, numPartitions);
                }
            }
            if(useIndex) {
                spatiotemporalPointRDD.buildIndex(indexType, true);
            }
        } else {
            if(isEstimation) {
                if(spatiotemporalPointRDD.rawRDD.getNumPartitions() < numPartitions) {
                    spatiotemporalPointRDD.rawRDD.repartition(numPartitions);
                }
                spatiotemporalPointRDD.generateCuboidPartitioner4RawRDD();
                initObservedPointDF(sparkSession, spatiotemporalPointRDD.rawRDD);
            } else {
                if(useCache) {
                    spatiotemporalPointRDD.setPartitioner(estimationPartitioner);
                    spatiotemporalPointRDD.rawRDD = spatiotemporalPointRDD.partition(estimationPartitioner);
                } else {
                    spatiotemporalPointRDD.generateCuboidPartitioner4RawRDD();
                }
            }
            if(useIndex) {
                spatiotemporalPointRDD.buildIndex(indexType, false);
            }
        }
    }

    private static void initObservedPointDF(SparkSession sparkSession, JavaRDD<SpatiotemporalPoint> spatiotemporalPointJavaRDD) {
        observedPointDF = sparkSession.createDataFrame(spatiotemporalPointJavaRDD.map(spatiotemporalPoint ->
            {
                Point spatialPoint = (Point) spatiotemporalPoint.getSpatialGeometry();
                return RowFactory.create(spatialPoint.getX(), spatialPoint.getY(), spatiotemporalPoint.getStartTime().toString());
            }
        ), spatiotemporalPointSchema).persist(StorageLevel.MEMORY_AND_DISK());
    }
    private static void buildSpatiotemporalWeightMap(JavaPairRDD<SpatiotemporalGeometry, SpatiotemporalPoint> pairRDD,
                                                                             double maxSpatialDistance, long maxTemporalDistance,
                                                                             TemporalUnit temporalUnit,int spatialStep,
                                                                             int temporalStep, SpatiotemporalPolygon spatiotemporalBoundary,
                                                                             EdgeCorrectionType edgeCorrectionType) throws Exception {

        /* build spatiotemporal weight map */
        JavaRDD<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>> mapAndK=
                pairRDD.mapPartitions(pairIterator -> {
                    List<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>> result = new ArrayList<>();
                    Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>> partitionWeightMap = new Tuple2<>(new HashMap<>(), new HashMap<>());
                    // initial partition K values
                    while(pairIterator.hasNext()) {
                        Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> pair = pairIterator.next();
                        Point centerPoint = (Point) pair._1.getSpatialGeometry();
                        Point neighborPoint = (Point) pair._2.getSpatialGeometry();

                        double pairSpatialDistance = calSpatialDistance(centerPoint, neighborPoint);

                        Point roundCenterPoint = geometryFactory.createPoint(new Coordinate(Math.round(centerPoint.getX()/spatialLocationTolerance)*spatialLocationTolerance,
                                Math.round(centerPoint.getY()/spatialLocationTolerance)*spatialLocationTolerance));
                        long roundSpatialDistance = Math.round(pairSpatialDistance/spatialDistanceTolerance)*spatialDistanceTolerance;

                        // put spatialWeight
                        if(!partitionWeightMap._1.containsKey(roundCenterPoint)) {
                            partitionWeightMap._1.put(roundCenterPoint, new HashMap<>());
                        }
                        if(!partitionWeightMap._1.get(roundCenterPoint).containsKey(roundSpatialDistance)) {
                            double spatialWeight;
                            spatialWeight = computeSpatialWeight(centerPoint, neighborPoint,
                                    (Polygon) spatiotemporalBoundary.getSpatialGeometry(), edgeCorrectionType);
                            partitionWeightMap._1.get(roundCenterPoint).put(roundSpatialDistance, spatialWeight);
                        }

                    }
                    result.add(partitionWeightMap);
                    return result.iterator();
                }, true);

        spatiotemporalWeightMapRDD = mapAndK.persist(StorageLevel.MEMORY_AND_DISK());
}
//    private static JavaRDD<Tuple2<Point,Double[]>> computeKAndBuildSpatiotemporalWeightMap(JavaPairRDD<SpatiotemporalGeometry, SpatiotemporalPoint> pairRDD,
//                                                                               double maxSpatialDistance, long maxTemporalDistance,
//                                                                               TemporalUnit temporalUnit,int spatialStep,
//                                                                               int temporalStep, SpatiotemporalPolygon spatiotemporalBoundary,
//                                                                               EdgeCorrectionType edgeCorrectionType,double intensity) throws Exception {
//
//       buildSpatiotemporalWeightMap(pairRDD, maxSpatialDistance, maxTemporalDistance,
//               temporalUnit, spatialStep, temporalStep, spatiotemporalBoundary, edgeCorrectionType);
//
//        JavaRDD<Tuple2<Point,Double[]>> KValuesRDD = pairRDD.zipPartitions(spatiotemporalWeightMapRDD, new FlatMapFunction2<Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>>, Iterator<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>>, Tuple2<Point,Double[]>>() {
//            @Override
//            public Iterator<Tuple2<Point,Double[]>> call(Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>> pointPairIterator, Iterator<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>> mapIterator) throws Exception {
//                List<Tuple2<Point,Double[]>> result = new ArrayList<>();
//                HashMap<Point, Double[]> mp = new HashMap<>();
//                if(!pointPairIterator.hasNext() || !mapIterator.hasNext()) {
//                    return result.iterator();
//                }
//                Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>> partitionWeightMap = mapIterator.next();
//                while(pointPairIterator.hasNext()) {
//                    Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> pointPair = pointPairIterator.next();
//                    Point centerPoint = (Point) pointPair._1.getSpatialGeometry();
//                    Point neighborPoint = (Point) pointPair._2.getSpatialGeometry();
//                    //LocalDateTime centerTime = LocalDateTime.parse(getSimplifiedTime(pointPair._1.getStartTime(), temporalUnit));
//                    //LocalDateTime neighborTime = LocalDateTime.parse(getSimplifiedTime(pointPair._2.getStartTime(), temporalUnit));
//
//                    double pairSpatialDistance = calSpatialDistance(centerPoint, neighborPoint);
//                    //long pairTemporalDistance = calTemporalDistance(centerTime, neighborTime, temporalUnit);
//
//                    Point roundCenterPoint = geometryFactory.createPoint(new Coordinate(Math.round(centerPoint.getX()/spatialLocationTolerance)*spatialLocationTolerance,
//                            Math.round(centerPoint.getY()/spatialLocationTolerance)*spatialLocationTolerance));
//                    long roundSpatialDistance = Math.round(pairSpatialDistance/spatialDistanceTolerance)*spatialDistanceTolerance;
//
//                    // get spatial weight
//                    double spatialWeight;
//                    if(!partitionWeightMap._1.containsKey(roundCenterPoint) || !partitionWeightMap._1.get(roundCenterPoint).containsKey(roundSpatialDistance)) {
//                        spatialWeight = computeSpatialWeight(centerPoint, neighborPoint,
//                                (Polygon) spatiotemporalBoundary.getSpatialGeometry(), edgeCorrectionType);
//                    } else {
//                        spatialWeight = partitionWeightMap._1.get(roundCenterPoint).get(roundSpatialDistance);
//                    }
//
//
//                    double spatiotemporalWeight = spatialWeight;
//
//                    Double[] partitionKValues;
//                    if (mp.containsKey(centerPoint)) {
//                        partitionKValues = mp.get(centerPoint);
//                    } else {
//                        partitionKValues=  new Double[spatialStep + 1];
//                        Arrays.fill(partitionKValues, 0d);
//                    }
//                    for(int i=spatialStep; i>0; i--) {
//                        double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
//                        if(pairSpatialDistance > currentSpatialDistance) {
//                            break;
//                        }
//                        partitionKValues[i] += spatiotemporalWeight;
//                    }
//                    for(int i=1; i<spatialStep + 1; i++) {
//                        Double originalKValue = partitionKValues[i];
//                        // sub self
//                        Double finalKValue = (originalKValue - 1) / (intensity);
//                        partitionKValues[i] = Math.sqrt(finalKValue / (Math.PI)) - (maxSpatialDistance / spatialStep) * i;
//                    }
//                    mp.put(centerPoint, partitionKValues);
//                }
//                for (Point pt: mp.keySet()) {
//                    result.add(new Tuple2(pt,mp.get(pt)));
//                }
//                return result.iterator();
//            }
//        });
//        return KValuesRDD;
//    }

    private static JavaPairRDD<SpatiotemporalGeometry, SpatiotemporalPoint> findQualifiedPairs(
            SpatiotemporalPointRDD spatiotemporalPointRDD, double currentSpatialDistance, long currentTemporalDistance,
            TemporalUnit temporalUnit, boolean usePartition, boolean useIndex) throws Exception {

        /* Create Circle for DistanceJoin */
        SpatiotemporalCircleRDD spatiotemporalCircleRDD = new SpatiotemporalCircleRDD(spatiotemporalPointRDD,
                currentSpatialDistance, currentTemporalDistance, temporalUnit);

        /* Partitioning the Circle for correctness of the result */
        if(usePartition && spatiotemporalCircleRDD.partitionedRDD == null) {
            spatiotemporalCircleRDD.spatiotemporalPartitioning(spatiotemporalPointRDD.getPartitioner());
        } else if(!usePartition) {
            spatiotemporalCircleRDD.rawRDD = spatiotemporalCircleRDD.partition(spatiotemporalPointRDD.getPartitioner());
        }

        /* Distance join circle and point */
        return JoinQuery.FlatDistanceJoinQuery(spatiotemporalCircleRDD,
                spatiotemporalPointRDD, usePartition, useIndex, true);
    }

    private static double computeSpatialWeight(Point centerPoint, Point neighborPoint, Polygon spatialBoundary, EdgeCorrectionType edgeCorrectionType) {
        switch (edgeCorrectionType) {
            case isotropic: {
                return EdgeCorrection.spatialIsotropicCorrection(centerPoint, neighborPoint, spatialBoundary);
            }
            default: {
                throw new UnsupportedOperationException("Unsupported edge correction type: " + edgeCorrectionType);
            }
        }
    }

    private static double computeTemporalWeight(LocalDateTime centerTime, LocalDateTime neighborTime,
                                                LocalDateTime startTime, LocalDateTime endTime, EdgeCorrectionType edgeCorrectionType) {
        switch (edgeCorrectionType) {
            case isotropic: {
                return EdgeCorrection.temporalIsotropicCorrection(centerTime, neighborTime, startTime, endTime);
            }
            default: {
                throw new UnsupportedOperationException("Unsupported edge correction type: " + edgeCorrectionType);
            }
        }
    }

    private static double computeWeight(SpatiotemporalPoint centerPoint, SpatiotemporalPoint neighborPoint,
                                               SpatiotemporalPolygon spatiotemporalBoundary, EdgeCorrectionType edgeCorrectionType) {
        switch (edgeCorrectionType) {
            case isotropic: {
                return EdgeCorrection.isotropicCorrection(centerPoint, neighborPoint, spatiotemporalBoundary);
            }
            default: {
                throw new UnsupportedOperationException("Unsupported edge correction type: " + edgeCorrectionType);
            }
        }
    }

    private static SpatiotemporalPointRDD simulateSpatiotemporalPoint(SparkSession sparkSession, int simulatedPointCount, SimulationType simulationType) {
        switch (simulationType) {
            case bootstrapping: {
                JavaRDD<SpatiotemporalPoint> sampledRDD = observedPointDF.sample(true, 2.0).
                        limit(simulatedPointCount).toJavaRDD().map(row -> {
                    double x = row.getAs("x");
                    double y = row.getAs("y");
                    String time = row.getAs("time");
                    LocalDateTime dateTime = LocalDateTime.parse(time);
                    return new SpatiotemporalPoint(new Coordinate(x, y), dateTime);
                });
                return new SpatiotemporalPointRDD(sampledRDD);
            }
            case randomPermutation: {
                Dataset<Row> randpermDF = observedPointDF.select(functions.col("time").as("randperm")).orderBy(functions.rand());
                JavaRDD<SpatiotemporalPoint> sampledRDD = addIndex(sparkSession, observedPointDF).
                        join(addIndex(sparkSession, randpermDF), "_index").
                        drop("_index").toJavaRDD().map(row -> {
                    double x = row.getAs("x");
                    double y = row.getAs("y");
                    String time = row.getAs("randperm");
                    LocalDateTime dateTime = LocalDateTime.parse(time);
                    return new SpatiotemporalPoint(new Coordinate(x, y), dateTime);
                });
                return new SpatiotemporalPointRDD(sampledRDD);
            }
            default: {
                throw new UnsupportedOperationException("Unsupported simulation type: " + simulationType);
            }
        }
    }

    private static Dataset<Row> addIndex(SparkSession sparkSession, Dataset<Row> dataFrame) {
        return sparkSession.createDataFrame(dataFrame.javaRDD().zipWithIndex().map(tuple -> {
                    Object[] fields = new Object[tuple._1.size()+1];
                    for(int i=0; i<fields.length-1; i++) {
                        fields[i] = tuple._1.get(i);
                    }
                    fields[fields.length-1] = tuple._2;
                    return RowFactory.create(fields);
                }),
                dataFrame.schema().add("_index", "long") );
    }

    private static double calSpatialDistance(Point point1, Point point2) {
        return Math.sqrt((point1.getX() - point2.getX())*(point1.getX() - point2.getX()) +
                (point1.getY() - point2.getY())*(point1.getY() - point2.getY()));
    }

    private static long calTemporalDistance(LocalDateTime dateTime1, LocalDateTime dateTime2, TemporalUnit temporalUnit) {
        if(temporalUnit instanceof ChronoUnit) {
            ChronoUnit chronoUnit = (ChronoUnit) temporalUnit;
            switch (chronoUnit) {
                case YEARS: {
                    return ChronoUnit.YEARS.between(dateTime1.toLocalDate(), dateTime2.toLocalDate());
                }
                case MONTHS: {
                    return ChronoUnit.MONTHS.between(dateTime1.toLocalDate(), dateTime2.toLocalDate());
                }
                case DAYS: {
                    return ChronoUnit.DAYS.between(dateTime1.toLocalDate(), dateTime2.toLocalDate());
                }
                case HOURS: {
                    return Duration.between(dateTime1, dateTime2).abs().toHours();
                }
                case MINUTES: {
                    return Duration.between(dateTime1, dateTime2).abs().toMinutes();
                }
                case SECONDS: {
                    return Duration.between(dateTime1, dateTime2).abs().getSeconds();
                }
            }
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + temporalUnit);
        }
        return temporalUnit.between(dateTime1, dateTime2);
    }

    private static String getSimplifiedTime(LocalDateTime dateTime, TemporalUnit temporalUnit) {
        if(temporalUnit instanceof ChronoUnit) {
            ChronoUnit chronoUnit = (ChronoUnit) temporalUnit;
            switch (chronoUnit) {
                case YEARS: {
                    return LocalDateTime.of(dateTime.getYear(), 1, 1, 0, 0).toString();
                }
                case MONTHS: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), 1, 0, 0).toString();
                }
                case DAYS: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            0, 0, 0).toString();
                }
                case HOURS: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            dateTime.getHour(), 0, 0).toString();
                }
                case MINUTES: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            dateTime.getHour(), dateTime.getMinute(), 0).toString();
                }
                case SECONDS: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()).toString();
                }
            }
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + temporalUnit);
        }
        return dateTime.toString();
    }
}
