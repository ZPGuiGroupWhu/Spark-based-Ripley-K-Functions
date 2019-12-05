package org.wysrc.distributedRipleysK.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalCircle;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalGeometry;
import org.wysrc.distributedRipleysK.operator.joinFunction.JoinWithLeftIndexFunction;
import org.wysrc.distributedRipleysK.operator.joinFunction.JoinWithRightIndexFunction;
import org.wysrc.distributedRipleysK.operator.joinFunction.JoinWithoutIndexFunction;
import org.wysrc.distributedRipleysK.partitioner.SpatiotemporalPartitioner;
import org.wysrc.distributedRipleysK.spatiotemporalRDD.SpatiotemporalCircleRDD;
import org.wysrc.distributedRipleysK.spatiotemporalRDD.SpatiotemporalRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

/**
 * Join Query Operator for SpatiotemporalGeometryRDD
 */
public class JoinQuery {
    private static final Logger log = LogManager.getLogger(JoinQuery.class);

    /**
     * Check if the two RDD are in the same CRS condition
     */
    private static <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry>
        void checkCRSMatch(SpatiotemporalRDD<T> leftRDD, SpatiotemporalRDD<U> rightRDD) {
        if(leftRDD.isCRSTransformed() != rightRDD.isCRSTransformed()) {
            throw new IllegalArgumentException("[JoinQuery] input RDDs aren't both transformed or untransformed in CRS, please check their constructor.");
        }
        if(leftRDD.isCRSTransformed() && rightRDD.isCRSTransformed() && !leftRDD.getTargetEPSGCode().equalsIgnoreCase(rightRDD.getTargetEPSGCode())) {
            throw new IllegalArgumentException("[JoinQuery] the EPSG code of input RDDs are different, please check their constructor.");
        }
    }

    private static <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry>
        void checkPartitionMatch(SpatiotemporalRDD<T> leftRDD, SpatiotemporalRDD<U> rightRDD) {
        Objects.requireNonNull(leftRDD.partitionedRDD, "[JoinQuery] the leftRDD isn't spatiotemporally partitioned, please perform repartition before join.");
        Objects.requireNonNull(rightRDD.partitionedRDD, "[JoinQuery] the rightRDD isn't spatiotemporally partitioned, please perform repartition before join.");

        final SpatiotemporalPartitioner leftPartitioner = leftRDD.getPartitioner();
        final SpatiotemporalPartitioner rightPartitioner = rightRDD.getPartitioner();

        if(! leftPartitioner.equals(rightPartitioner)) {
            throw new IllegalArgumentException("[JoinQuery] input RDDs aren't partitioned by the same cuboids, please make sure they use the same partitioner.");
        }

        final int leftNumPartition = leftRDD.partitionedRDD.getNumPartitions();
        final int rightNumPartition = rightRDD.partitionedRDD.getNumPartitions();
        if(leftNumPartition != rightNumPartition) {
            throw new IllegalArgumentException("[JoinQuery] numbers of partitions from input RDDs are different (left: "+leftNumPartition+", right: "+rightNumPartition+"), please make sure they use the same partitioner.");
        }
    }

    private static <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry>
        JavaPairRDD<T, ArrayList<U>> collectPairByKey(JavaPairRDD<T, U> pairRDD) {
        return pairRDD.aggregateByKey(
            new ArrayList<>(),
            new Function2<ArrayList<U>, U, ArrayList<U>>() {
                @Override
                public ArrayList<U> call(ArrayList<U> list, U u) {
                    list.add(u);
                    return list;
                }
            },
            new Function2<ArrayList<U>, ArrayList<U>, ArrayList<U>>() {
                @Override
                public ArrayList<U> call(ArrayList<U> list, ArrayList<U> list2) {
                    list.addAll(list2);
                    return list;
                }
            }
        );
    }

    private static <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry>
        JavaPairRDD<T, Long> countPairByKey(JavaPairRDD<T, U> pairRDD) {
        return pairRDD.aggregateByKey(
            0L,
            new Function2<Long, U, Long>() {
                @Override
                public Long call(Long count, U u) {
                    return count + 1;
                }
            },
            new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long count1, Long count2) {
                    return count1 + count2;
                }
            }
        );
    }

    public static <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry> JavaPairRDD<T, ArrayList<U>>
        SpatiotemporalJoinQuery(SpatiotemporalRDD<T> leftRDD, SpatiotemporalRDD<U> rightRDD,
                                boolean usePartition, boolean useIndex, boolean includeBoundary) throws Exception {
        final JavaPairRDD<T, U> joinResult = spatiotemporalJoin(leftRDD, rightRDD, usePartition, useIndex, includeBoundary);
        return collectPairByKey(joinResult);
    }

    public static <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry> JavaPairRDD<T, Long>
        CountSpatiotemporalJoinQuery(SpatiotemporalRDD<T> leftRDD, SpatiotemporalRDD<U> rightRDD,
                                     boolean usePartition, boolean useIndex, boolean includeBoundary) throws Exception {
        final JavaPairRDD<T, U> joinResult = spatiotemporalJoin(leftRDD, rightRDD, usePartition, useIndex, includeBoundary);
        return countPairByKey(joinResult);
    }

    public static <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry> JavaPairRDD<T, U>
        FlatSpatiotemporalJoinQuery(SpatiotemporalRDD<T> leftRDD, SpatiotemporalRDD<U> rightRDD,
                                    boolean usePartition, boolean useIndex, boolean includeBoundary) throws Exception {
        return spatiotemporalJoin(leftRDD, rightRDD, usePartition, useIndex, includeBoundary);
    }

    public static <T extends SpatiotemporalGeometry> JavaPairRDD<SpatiotemporalGeometry, ArrayList<T>>
        DistanceJoinQuery(SpatiotemporalCircleRDD circleRDD, SpatiotemporalRDD<T> geometryRDD,
                          boolean usePartition, boolean useIndex, boolean includeBoundary) throws Exception {
        final JavaPairRDD<SpatiotemporalGeometry, T> joinResult = distanceJoin(circleRDD, geometryRDD, usePartition, useIndex, includeBoundary);
        return collectPairByKey(joinResult);
    }

    public static <T extends SpatiotemporalGeometry> JavaPairRDD<SpatiotemporalGeometry, Long>
        CountDistanceJoinQuery(SpatiotemporalCircleRDD circleRDD, SpatiotemporalRDD<T> geometryRDD,
                               boolean usePartition, boolean useIndex, boolean includeBoundary) throws Exception {
        final JavaPairRDD<SpatiotemporalGeometry, T> joinResult = distanceJoin(circleRDD, geometryRDD, usePartition, useIndex, includeBoundary);
        return countPairByKey(joinResult);
    }

    public static <T extends SpatiotemporalGeometry> JavaPairRDD<SpatiotemporalGeometry, T>
        FlatDistanceJoinQuery(SpatiotemporalCircleRDD circleRDD, SpatiotemporalRDD<T> geometryRDD,
                              boolean usePartition, boolean useIndex, boolean includeBoundary) throws Exception {
        return distanceJoin(circleRDD, geometryRDD, usePartition, useIndex, includeBoundary);
    }

    public static <T extends SpatiotemporalGeometry>
        JavaPairRDD<SpatiotemporalGeometry, T> distanceJoin(SpatiotemporalCircleRDD circleRDD, SpatiotemporalRDD<T> geometryRDD,
                                                            boolean usePartition, boolean useIndex, boolean includeBoundary) throws Exception {
        JavaPairRDD<SpatiotemporalCircle, T> joinResult = spatiotemporalJoin(circleRDD, geometryRDD, usePartition, useIndex, includeBoundary);
        return joinResult.mapToPair(new PairFunction<Tuple2<SpatiotemporalCircle, T>, SpatiotemporalGeometry, T>() {
            @Override
            public Tuple2<SpatiotemporalGeometry, T> call(Tuple2<SpatiotemporalCircle, T> spatiotemporalCircleTTuple2) {
                return new Tuple2<>(spatiotemporalCircleTTuple2._1().getCenterSpatiotemporalGeometry(), spatiotemporalCircleTTuple2._2());
            }
        });
    }

    public static <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry>
        JavaPairRDD<T, U> spatiotemporalJoin(SpatiotemporalRDD<T> leftRDD, SpatiotemporalRDD<U> rightRDD,
                                             boolean usePartition, boolean useIndex, boolean includeBoundary) {
        checkCRSMatch(leftRDD, rightRDD);

        final JavaRDD<Pair<T, U>> joinResult;
        if(usePartition) {
            checkPartitionMatch(leftRDD, rightRDD);
            if(useIndex) {
                if(leftRDD.indexedPartitionedRDD != null) {
                    final JoinWithLeftIndexFunction joinFunction = new JoinWithLeftIndexFunction(includeBoundary);
                    joinResult = rightRDD.partitionedRDD.zipPartitions(leftRDD.indexedPartitionedRDD, joinFunction);
                } else if(rightRDD.indexedPartitionedRDD != null) {
                    final JoinWithRightIndexFunction joinFunction = new JoinWithRightIndexFunction(includeBoundary);
                    joinResult = leftRDD.partitionedRDD.zipPartitions(rightRDD.indexedPartitionedRDD, joinFunction);
                } else {
                    throw new IllegalArgumentException("[JoinQuery] UseIndex is set to true, but no index has been built on any one of the RDDs.");
                }
            } else {
                JoinWithoutIndexFunction joinFunction = new JoinWithoutIndexFunction(includeBoundary);
                joinResult = leftRDD.partitionedRDD.zipPartitions(rightRDD.partitionedRDD, joinFunction);
            }
        } else {
            if(useIndex) {
                if(leftRDD.indexedRawRDD != null) {
                    final JoinWithLeftIndexFunction joinFunction = new JoinWithLeftIndexFunction(includeBoundary);
                    joinResult = rightRDD.rawRDD.zipPartitions(leftRDD.indexedRawRDD, joinFunction);
                } else if(rightRDD.indexedRawRDD != null) {
                    final JoinWithRightIndexFunction joinFunction = new JoinWithRightIndexFunction(includeBoundary);
                    joinResult = leftRDD.rawRDD.zipPartitions(rightRDD.indexedRawRDD, joinFunction);
                } else {
                    throw new IllegalArgumentException("[JoinQuery] UseIndex is set to true, but no index has been built on any one of the RDDs.");
                }
            } else {
                JoinWithoutIndexFunction joinFunction = new JoinWithoutIndexFunction(includeBoundary);
                joinResult = leftRDD.rawRDD.zipPartitions(rightRDD.rawRDD, joinFunction);
            }
        }

        return joinResult.mapToPair(new PairFunction<Pair<T, U>, T, U>() {
            @Override
            public Tuple2<T, U> call(Pair<T, U> pair) {
                return new Tuple2<>(pair.getKey(), pair.getValue());
            }
        });
    }
}
