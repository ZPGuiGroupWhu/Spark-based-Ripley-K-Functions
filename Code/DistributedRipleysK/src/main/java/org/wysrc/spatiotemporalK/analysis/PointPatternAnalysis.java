package org.wysrc.spatiotemporalK.analysis;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.wysrc.spatiotemporalK.geom.SpatiotemporalGeometry;
import org.wysrc.spatiotemporalK.geom.SpatiotemporalPoint;
import org.wysrc.spatiotemporalK.geom.SpatiotemporalPolygon;
import org.wysrc.spatiotemporalK.index.IndexType;
import org.wysrc.spatiotemporalK.operator.JoinQuery;
import org.wysrc.spatiotemporalK.partitioner.PartitionerType;
import org.wysrc.spatiotemporalK.partitioner.SpatiotemporalPartitioner;
import org.wysrc.spatiotemporalK.spatiotemporalRDD.SpatiotemporalCircleRDD;
import org.wysrc.spatiotemporalK.spatiotemporalRDD.SpatiotemporalPointRDD;
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
        List Kmin = new ArrayList(spatialStep + 1);
        List Kmax = new ArrayList(spatialStep + 1);
        for(int i=0; i<spatialStep+1; i++) {
            if(i == 0) {
                List<Double> K = new ArrayList<>(Collections.nCopies(temporalStep + 1, 0d));
                Kmin.add(K);
                Kmax.add(K);
                continue;
            }

            List<Double> initialKmin = new ArrayList<>(Collections.nCopies(temporalStep + 1, Double.MAX_VALUE));
            List<Double> initialKmax = new ArrayList<>(Collections.nCopies(temporalStep + 1, Double.MIN_VALUE));
            initialKmin.set(0, 0d);
            initialKmax.set(0, 0d);
            Kmin.add(initialKmin);
            Kmax.add(initialKmax);
        }
        // get K value of simulated point pattern
        for(int s=0; s<simulationCount; s++) {
            // generate simulated points
            SpatiotemporalPointRDD simulatedPointRDD = simulateSpatiotemporalPoint(sparkSession,
                    (int) spatiotemporalPointRDD.totalCount, simulationType);
            simulatedPointRDD.totalCount = spatiotemporalPointRDD.totalCount;
            // organize the simulated points
            organizeSpatiotemporalPointRDD(sparkSession, simulatedPointRDD, partitionerType, numPartitions, indexType,
                    spatiotemporalPointRDD.getPartitioner(), usePartition, useIndex, useCache, false);
            // execute K function on simulated points
            List Ksim = SpatiotemporalRipleysKFunction4SinglePointPattern(simulatedPointRDD, maxSpatialDistance,
                    maxTemporalDistance, temporalUnit, spatiotemporalBoundary, spatialStep, temporalStep, edgeCorrectionType,
                    usePartition, useIndex, useCache, false);
            for(int j=0; j<spatialStep+1; j++) {
                for(int k=0; k<temporalStep+1; k++) {
                    double KsimValue = (double) ((List) Ksim.get(j)).get(k);
                    double KminValue = (double) ((List) Kmin.get(j)).get(k);
                    double KmaxValue = (double) ((List) Kmax.get(j)).get(k);
                    if(KsimValue < KminValue) {
                        ((List) Kmin.get(j)).set(k, KsimValue);
                    }
                    if(KsimValue > KmaxValue) {
                        ((List) Kmax.get(j)).set(k, KsimValue);
                    }
                }
            }
        }

        return new RipleysKResult(Kest, Kmin, Kmax, maxSpatialDistance, maxTemporalDistance, temporalUnit);
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
        JavaRDD<Double[][]> KValuesRDD;
        if(useCache) {
            if(isEstimation) {
                KValuesRDD = computeKAndBuildSpatiotemporalWeightMap(pairRDD, maxSpatialDistance, maxTemporalDistance,
                        temporalUnit, spatialStep, temporalStep, spatiotemporalBoundary, edgeCorrectionType);
            } else {
                KValuesRDD = pairRDD.zipPartitions(spatiotemporalWeightMapRDD, new FlatMapFunction2<Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>>, Iterator<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>>, Double[][]>() {
                    @Override
                    public Iterator<Double[][]> call(Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>> pointPairIterator, Iterator<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>> mapIterator) throws Exception {
                        List<Double[][]> result = new ArrayList<>();
                        if(!pointPairIterator.hasNext() || !mapIterator.hasNext()) {
                            return result.iterator();
                        }

                        // initial partition K values
                        Double[][] partitionKValues = new Double[spatialStep + 1][temporalStep + 1];
                        for(Double[] partitionKValuesAtSpatial: partitionKValues) {
                            Arrays.fill(partitionKValuesAtSpatial, 0d);
                        }

                        Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>> partitionWeightMap = mapIterator.next();
                        while(pointPairIterator.hasNext()) {
                            Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> pointPair = pointPairIterator.next();
                            Point centerPoint = (Point) pointPair._1.getSpatialGeometry();
                            Point neighborPoint = (Point) pointPair._2.getSpatialGeometry();
                            LocalDateTime centerTime = LocalDateTime.parse(getSimplifiedTime(pointPair._1.getStartTime(), temporalUnit));
                            LocalDateTime neighborTime = LocalDateTime.parse(getSimplifiedTime(pointPair._2.getStartTime(), temporalUnit));

                            double pairSpatialDistance = calSpatialDistance(centerPoint, neighborPoint);
                            long pairTemporalDistance = calTemporalDistance(centerTime, neighborTime, temporalUnit);

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

                            // get temporal weight
                            double temporalWeight;
                            if(!partitionWeightMap._2.containsKey(centerTime) || !partitionWeightMap._2.get(centerTime).containsKey(pairTemporalDistance)) {
                                temporalWeight = computeTemporalWeight(centerTime, neighborTime,
                                        spatiotemporalBoundary.getStartTime(), spatiotemporalBoundary.getEndTime(), edgeCorrectionType);
                            } else {
                                temporalWeight = partitionWeightMap._2.get(centerTime).get(pairTemporalDistance);
                            }

                            double spatiotemporalWeight = spatialWeight * temporalWeight;
                            for(int i=spatialStep; i>0; i--) {
                                double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
                                if(pairSpatialDistance > currentSpatialDistance) {
                                    break;
                                }
                                for(int j=temporalStep; j>0; j--) {
                                    long currentTemporalDistance = (maxTemporalDistance / temporalStep) * j;
                                    if(pairTemporalDistance > currentTemporalDistance) {
                                        break;
                                    }
                                    partitionKValues[i][j] += spatiotemporalWeight;
                                }
                            }
                        }
                        result.add(partitionKValues);
                        return result.iterator();
                    }
                });
            }
        } else {
            KValuesRDD = pairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>>, Double[][]>() {
                @Override
                public Iterator<Double[][]> call(Iterator<Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint>> pointPairIterator) throws Exception {
                    List<Double[][]> result = new ArrayList<>();
                    if(!pointPairIterator.hasNext()) {
                        return result.iterator();
                    }

                    // initial partition K values
                    Double[][] partitionKValues = new Double[spatialStep + 1][temporalStep + 1];
                    for(Double[] partitionKValuesAtSpatial: partitionKValues) {
                        Arrays.fill(partitionKValuesAtSpatial, 0d);
                    }

                    while(pointPairIterator.hasNext()) {
                        Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> pointPair = pointPairIterator.next();
                        double spatiotemporalWeight = computeWeight((SpatiotemporalPoint) pointPair._1(), pointPair._2(),
                                spatiotemporalBoundary, edgeCorrectionType);
                        double pairSpatialDistance = calSpatialDistance((Point) pointPair._1().getSpatialGeometry(),
                                (Point) pointPair._2().getSpatialGeometry());
                        long pairTemporalDistance = calTemporalDistance(pointPair._1().getStartTime(),
                                pointPair._2().getStartTime(), temporalUnit);
                        for(int i=spatialStep; i>0; i--) {
                            double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
                            if(pairSpatialDistance > currentSpatialDistance) {
                                break;
                            }
                            for(int j=temporalStep; j>0; j--) {
                                long currentTemporalDistance = (maxTemporalDistance / temporalStep) * j;
                                if(pairTemporalDistance > currentTemporalDistance) {
                                    break;
                                }
                                partitionKValues[i][j] += spatiotemporalWeight;
                            }
                        }
                    }
                    result.add(partitionKValues);
                    return result.iterator();
                }
            }, true);
        }

        Double[][] combinedKValues = KValuesRDD.reduce(new Function2<Double[][], Double[][], Double[][]>() {
            @Override
            public Double[][] call(Double[][] doublesA, Double[][] doublesB) throws Exception {
                for(int i=0; i<spatialStep+1; i++) {
                    for(int j=0; j<temporalStep+1; j++) {
                        doublesA[i][j] +=  doublesB[i][j];
                    }
                }
                return doublesA;
            }
        });

        List<List<Double>> finalResult = new ArrayList<>(spatialStep + 1);
        // skip the first row of 0 values
        finalResult.add(new ArrayList<>(Collections.nCopies(temporalStep + 1, 0d)));
        for(int i=1; i<spatialStep + 1; i++) {
            finalResult.add(new ArrayList<>(Collections.nCopies(temporalStep + 1, 0d)));
            for(int j=1; j<temporalStep+1; j++) {
                Double originalKValue = combinedKValues[i][j];
                Double finalKValue = (originalKValue - spatiotemporalPointRDD.totalCount) / (spatiotemporalPointRDD.totalCount * intensity);
                Double finalLValue = Math.sqrt(finalKValue/(Math.PI*2*(maxTemporalDistance/temporalStep)*j)) - (maxSpatialDistance/spatialStep)*i;
                finalResult.get(i).set(j, finalLValue);
            }
        }
        return finalResult;
    }

    private static void organizeSpatiotemporalPointRDD(SparkSession sparkSession, SpatiotemporalPointRDD spatiotemporalPointRDD,
                                                       PartitionerType partitionerType, int numPartitions,
                                                       IndexType indexType, SpatiotemporalPartitioner estimationPartitioner,
                                                       boolean usePartition, boolean useIndex, boolean useCache, boolean isEstimation) throws Exception {
        if(usePartition) {
            if(isEstimation) {
                spatiotemporalPointRDD.spatiotemporalPartitioning(partitionerType, numPartitions);
                initObservedPointDF(sparkSession, spatiotemporalPointRDD.partitionedRDD);
            } else {
                if(useCache) {
                    spatiotemporalPointRDD.spatiotemporalPartitioning(estimationPartitioner);
                } else {
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

    private static JavaRDD<Double[][]> computeKAndBuildSpatiotemporalWeightMap(JavaPairRDD<SpatiotemporalGeometry, SpatiotemporalPoint> pairRDD,
                                                                               double maxSpatialDistance, long maxTemporalDistance,
                                                                               TemporalUnit temporalUnit,int spatialStep,
                                                                               int temporalStep, SpatiotemporalPolygon spatiotemporalBoundary,
                                                                               EdgeCorrectionType edgeCorrectionType) throws Exception {

        /* build spatiotemporal weight map */
        JavaPairRDD<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>, Double[][]> mapAndK=
        pairRDD.mapPartitionsToPair(pairIterator -> {
            List<Tuple2<Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>>, Double[][]>> result = new ArrayList<>();
            Tuple2<HashMap<Point, HashMap<Long, Double>>, HashMap<LocalDateTime, HashMap<Long, Double>>> partitionWeightMap = new Tuple2<>(new HashMap<>(), new HashMap<>());
            // initial partition K values
            Double[][] partitionKValues = new Double[spatialStep + 1][temporalStep + 1];
            for(Double[] partitionKValuesAtSpatial: partitionKValues) {
                Arrays.fill(partitionKValuesAtSpatial, 0d);
            }
            while(pairIterator.hasNext()) {
                Tuple2<SpatiotemporalGeometry, SpatiotemporalPoint> pair = pairIterator.next();
                Point centerPoint = (Point) pair._1.getSpatialGeometry();
                Point neighborPoint = (Point) pair._2.getSpatialGeometry();
                LocalDateTime centerTime = LocalDateTime.parse(getSimplifiedTime(pair._1.getStartTime(), temporalUnit));
                LocalDateTime neighborTime = LocalDateTime.parse(getSimplifiedTime(pair._2.getStartTime(), temporalUnit));

                double pairSpatialDistance = calSpatialDistance(centerPoint, neighborPoint);
                long pairTemporalDistance = calTemporalDistance(centerTime, neighborTime, temporalUnit);

                Point roundCenterPoint = geometryFactory.createPoint(new Coordinate(Math.round(centerPoint.getX()/spatialLocationTolerance)*spatialLocationTolerance,
                        Math.round(centerPoint.getY()/spatialLocationTolerance)*spatialLocationTolerance));
                long roundSpatialDistance = Math.round(pairSpatialDistance/spatialDistanceTolerance)*spatialDistanceTolerance;

                // put spatialWeight
                if(!partitionWeightMap._1.containsKey(roundCenterPoint)) {
                    partitionWeightMap._1.put(roundCenterPoint, new HashMap<>());
                }
                double spatialWeight;
                if(!partitionWeightMap._1.get(roundCenterPoint).containsKey(roundSpatialDistance)) {
                    spatialWeight = computeSpatialWeight(centerPoint, neighborPoint,
                            (Polygon) spatiotemporalBoundary.getSpatialGeometry(), edgeCorrectionType);
                    partitionWeightMap._1.get(roundCenterPoint).put(roundSpatialDistance, spatialWeight);
                } else {
                    spatialWeight = partitionWeightMap._1.get(roundCenterPoint).get(roundSpatialDistance);
                }

                // put temporalWeight
                if(!partitionWeightMap._2.containsKey(centerTime)) {
                    partitionWeightMap._2.put(centerTime, new HashMap<>());
                }
                double temporalWeight;
                if(!partitionWeightMap._2.get(centerTime).containsKey(pairTemporalDistance)) {
                    temporalWeight = computeTemporalWeight(centerTime, neighborTime,
                            spatiotemporalBoundary.getStartTime(), spatiotemporalBoundary.getEndTime(), edgeCorrectionType);
                    partitionWeightMap._2.get(centerTime).put(pairTemporalDistance, temporalWeight);
                } else {
                    temporalWeight = partitionWeightMap._2.get(centerTime).get(pairTemporalDistance);
                }

                double spatiotemporalWeight = spatialWeight * temporalWeight;
                for(int i=spatialStep; i>0; i--) {
                    double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
                    if(pairSpatialDistance > currentSpatialDistance) {
                        break;
                    }
                    for(int j=temporalStep; j>0; j--) {
                        long currentTemporalDistance = (maxTemporalDistance / temporalStep) * j;
                        if(pairTemporalDistance > currentTemporalDistance) {
                            break;
                        }
                        partitionKValues[i][j] += spatiotemporalWeight;
                    }
                }
            }
            result.add(new Tuple2<>(partitionWeightMap, partitionKValues));
            return result.iterator();
        }, true);
        spatiotemporalWeightMapRDD = mapAndK.keys().persist(StorageLevel.MEMORY_AND_DISK());
        return mapAndK.values();
    }

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
