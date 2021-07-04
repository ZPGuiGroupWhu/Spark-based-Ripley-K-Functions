package org.wysrc.crossK.spatiotemporalRDD;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.random.SamplingUtils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.wysrc.crossK.geom.SpatiotemporalEnvelope;
import org.wysrc.crossK.geom.SpatiotemporalGeometry;
import org.wysrc.crossK.geom.SpatiotemporalPoint;
import org.wysrc.crossK.index.IndexBuilder;
import org.wysrc.crossK.index.IndexType;
import org.wysrc.crossK.index.SpatiotemporalIndex;
import org.wysrc.crossK.index.kdbtree.KDBTree;
import org.wysrc.crossK.partitioner.CuboidPartitioner;
import org.wysrc.crossK.partitioner.KDBTreePartitioner;
import org.wysrc.crossK.partitioner.PartitionerType;
import org.wysrc.crossK.partitioner.SpatiotemporalPartitioner;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The extended RDD for spatiotemporal objects
 */
public class SpatiotemporalRDD<T extends SpatiotemporalGeometry> implements Serializable {
    /**
     * The constant Logger
     */
    static final Logger logger = Logger.getLogger(SpatiotemporalRDD.class);

    /**
     * When total count is less than the threshold, use total elements as sample.
     */
    static final int TOTAL_COUNT_THRESHOLD = 1000;

    /**
     * The raw RDD from input data source
     */
    public JavaRDD<T> rawRDD;

    /**
     * The spatiotemporally partitioned RDD
     */
    public JavaRDD<T> partitionedRDD;

    public JavaPairRDD<SpatiotemporalGeometry, SpatiotemporalPoint> beforePartitionPair;

    public JavaPairRDD<SpatiotemporalGeometry, SpatiotemporalPoint> afterPartitionPair;

    /**
     * The spatiotemporally indexed RDD that has NOT been partitioned
     */
    public JavaRDD<SpatiotemporalIndex> indexedRawRDD;

    /**
     * The spatiotemporally indexed RDD that has been partitioned
     */
    public JavaRDD<SpatiotemporalIndex> indexedPartitionedRDD;

    /**
     * The names of element fields
     */
    public List<String> fieldNames;

    /**
     * The spatiotemporal envelope of RDD
     */
    public SpatiotemporalEnvelope envelope = null;

    /**
     * The total count of elements
     */
    public long totalCount = -1;

    /**
     * The cuboids used to partition spatiotemporal elements
     */
    public List<SpatiotemporalEnvelope> cuboids;

    /**
     * Whether this RDD is analyzed for envelope and total count
     */
    protected boolean analyzed = false;

    /**
     * Whether this RDD is transformed in CRS
     */
    protected boolean CRSTransformed = false;

    /**
     * The source EPSG code of CRS
     */
    protected String sourceEPSGCode = "";

    /**
     * The target EPSG code of CRS
     */
    protected String targetEPSGCode = "";

    /**
     * The partitioner for this RDD
     */
    private SpatiotemporalPartitioner partitioner;

    /**
     * The sample count for partitioning
     */
    private int sampleCount = -1;

    public boolean isAnalyzed() {
        return analyzed;
    }

    public boolean isCRSTransformed() {
        return CRSTransformed;
    }

    public String getSourceEPSGCode() {
        return sourceEPSGCode;
    }

    public String getTargetEPSGCode() {
        return targetEPSGCode;
    }

    public SpatiotemporalPartitioner getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(SpatiotemporalPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    public int getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
    }

    /**
     * Transform the coordinates of RDD from source CRS to target CRS
     *
     * @param sourceEPSGCode the source EPSG Code
     * @param targetEPSGCode the target EPSG Code
     * @return whether the transform is finished
     */
    public boolean CRSTransform(String sourceEPSGCode, String targetEPSGCode) {
        try {
            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEPSGCode);
            CoordinateReferenceSystem targetCRS = CRS.decode(targetEPSGCode);
            final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
            this.CRSTransformed = true;
            this.sourceEPSGCode = sourceEPSGCode;
            this.targetEPSGCode = targetEPSGCode;
            this.rawRDD = this.rawRDD.map(new Function<T, T>() {
                @Override
                public T call(T spatiotemporalGeometry) throws Exception {
                    spatiotemporalGeometry.setSpatialGeometry(JTS.transform(spatiotemporalGeometry.getSpatialGeometry(), transform));
                    return spatiotemporalGeometry;
                }
            });

            return true;
        } catch (FactoryException e) {
            // TODO: more effective and proper catch
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Analyzes envelope and count of the spatiotemporalRDD
     */
    public void analyze() {
        if(this.envelope == null) {
            this.envelope = this.rawRDD.map(T -> T.getEnvelopeInternal()).reduce((envelope1, envelope2) -> {
                // TODO: decrease the object construction
                envelope1.expandToInclude(envelope2);
                return envelope1;
            });
        }
        if(this.totalCount == -1) {
            this.totalCount = this.rawRDD.count();
        }
        this.analyzed = true;
    }

    /**
     * Completes the analysis with known envelope and count
     * @param envelope known envelope
     * @param totalCount known count
     */
    public void analyze(SpatiotemporalEnvelope envelope, long totalCount) {
        this.envelope = envelope;
        this.totalCount = totalCount;
        this.analyzed = true;
    }

    /**
     * Persists the rawRDD with specific storageLevel and analyzes its envelope and count
     * @param storageLevel storageLevel for the rawRDD
     */
    public void analyze(StorageLevel storageLevel) {
        this.rawRDD = this.rawRDD.persist(storageLevel);
        this.analyze();
    }

    /**
     * Gets the envelope of this spatiotemporalRDD
     * @return spatiotemporal envelope
     */
    public SpatiotemporalEnvelope getEnvelope() {
        this.analyze();
        return this.envelope;
    }

    /**
     * Builds spatiotemporal partitioner and partitions the rawRDD with number of partitions from raw RDD
     *
     * @param partitionerType the partition type
     * @throws Exception exception from buildSpatiotemporalPartitioner
     */
    public void spatiotemporalPartitioning(PartitionerType partitionerType) throws Exception {
        buildSpatiotemporalPartitioner(partitionerType);
        this.partitionedRDD = partition(this.partitioner);
    }

    /**
     * Builds spatiotemporal partitioner and partitions the rawRDD
     *
     * @param partitionerType the partition type
     * @param numPartitions the number of partitions for partitionedRDD
     * @throws Exception exception from buildSpatiotemporalPartitioner
     */
    public void spatiotemporalPartitioning(PartitionerType partitionerType, int numPartitions) throws Exception {
        buildSpatiotemporalPartitioner(partitionerType, numPartitions);
        this.partitionedRDD = partition(this.partitioner);
    }

    /**
     * Sets a specific partitioner and partitions the rawRDD
     *
     * @param partitioner existing partitioner
     */
    public void spatiotemporalPartitioning(SpatiotemporalPartitioner partitioner) {
        this.partitioner = partitioner;
        this.partitionedRDD = partition(partitioner);
    }

    /**
     * Builds spatiotemporal partitioner with number of partitions from raw RDD
     *
     * @param partitionerType the partitioner type
     * @throws Exception exception from buildSpatiotemporalPartitioner
     */
    public void buildSpatiotemporalPartitioner(PartitionerType partitionerType) throws Exception {
        int numPartitions = this.rawRDD.getNumPartitions();
        buildSpatiotemporalPartitioner(partitionerType, numPartitions);
    }

    /**
     * Builds spatiotemporal partitioner
     *
     * @param partitionerType the partitioner type
     * @param numPartitions the numPartitions for the RDD after partitioning
     * @throws Exception exception from buildSpatiotemporalPartitioner
     */
    public void buildSpatiotemporalPartitioner(PartitionerType partitionerType, int numPartitions) throws Exception {
        if(numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        if(this.envelope == null || this.totalCount == -1) {
            // throw new Exception("[SpatiotemporalRDD][spatiotemporalPartitioning] Spatiotemporal envelope or total count is unknown, please call analyze() and do partitioning again.");
            analyze();
        }

        if(this.totalCount < numPartitions*2) {
            throw new Exception("[SpatiotemporalRDD][spatiotemporalPartitioner] Number of partitions (" + numPartitions + ") cannot be greater than half of total count (" + this.totalCount + ").");
        }

        List<T> samples;
        if(this.totalCount < TOTAL_COUNT_THRESHOLD) {
            samples = this.rawRDD.collect();

            logger.info("Collected total elements (" + samples.size() + ") as samples");
        } else {
            if(this.sampleCount < 0 || this.sampleCount > this.totalCount) {
                this.sampleCount = Math.max(numPartitions*2, Math.min((int) this.totalCount / 100, Integer.MAX_VALUE));
            }
            final double fraction = SamplingUtils.computeFractionForSampleSize(this.sampleCount, this.totalCount, false);
            samples = this.rawRDD.sample(false, fraction).collect();

            logger.info("Collected " + samples.size() + " samples");
        }

        switch (partitionerType) {
            case Equal: {
                break;
            }
            case OcTree: {
                break;
            }
            case STRTree: {
                break;
            }
            case KDBTree: {
                KDBTree kdbTree = new KDBTree(samples.size()/numPartitions, samples.size()/numPartitions, envelope);
                for(T sample: samples) {
                    kdbTree.insert(sample.getEnvelopeInternal(), sample);
                }
                kdbTree.assignLeafEnvelopeIds();
                this.partitioner = new KDBTreePartitioner(kdbTree);
                break;
            }
            default:
                throw new Exception("[SpatiotemporalRDD][spatiotemporalPartitioner] Unknown partitioner type, please check your input.");
        }
    }

    /**
     * Partition the rawRDD with specific partitioner
     *
     * @param partitioner existing partitioner
     * @return a new partitioned RDD
     */
    public JavaRDD<T> partition(SpatiotemporalPartitioner partitioner) {
        return this.rawRDD.flatMapToPair(
                new PairFlatMapFunction<T, Integer, T>()
                {
                    @Override
                    public Iterator<Tuple2<Integer, T>> call(T spatialObject)
                            throws Exception
                    {
                        return partitioner.divideObject(spatialObject);
                    }
                }
        ).partitionBy(partitioner).mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<Integer, T>>, T>()
                {
                    @Override
                    public Iterator<T> call(final Iterator<Tuple2<Integer, T>> tuple2Iterator)
                    {
                        return new Iterator<T>()
                        {
                            @Override
                            public boolean hasNext()
                            {
                                return tuple2Iterator.hasNext();
                            }

                            @Override
                            public T next()
                            {
                                return tuple2Iterator.next()._2();
                            }

                            @Override
                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                },
                true);
    }

    /**
     * Build index for partitions
     *
     * @param indexType the index type
     * @param buildOnPartitionedRDD whether build index on partitioned RDD
     * @throws Exception exception from building index
     */
    public void buildIndex(final IndexType indexType, boolean buildOnPartitionedRDD) throws Exception {
        if(!buildOnPartitionedRDD) {
            this.indexedRawRDD = this.rawRDD.mapPartitions(new IndexBuilder(indexType), true);
        } else {
            if(this.partitionedRDD == null) {
                throw new Exception("[SpatiotemporalRDD][buildIndex] RDD is not yet partitioned, please do partitioning before build index.");
            }
            this.indexedPartitionedRDD = this.partitionedRDD.mapPartitions(new IndexBuilder(indexType), true);
        }
    }

    /**
     * Generates a CuboidPartitioner for rawRDD of this SpatiotemporalRDD
     */
    public void generateCuboidPartitioner4RawRDD() {
        List<SpatiotemporalEnvelope> cuboids = this.rawRDD.map(g -> g.getEnvelopeInternal()).mapPartitionsWithIndex(
            new Function2<Integer, Iterator<SpatiotemporalEnvelope>, Iterator<SpatiotemporalEnvelope>>() {
                @Override
                public Iterator<SpatiotemporalEnvelope> call(Integer index, Iterator<SpatiotemporalEnvelope> spatiotemporalEnvelopeIterator) {
                    List<SpatiotemporalEnvelope> result = new ArrayList<>();
                    if(!spatiotemporalEnvelopeIterator.hasNext()) {
                        return result.iterator();
                    }
                    // merge the envelope of elements in the same partition
                    SpatiotemporalEnvelope partitionEnvelope = new SpatiotemporalEnvelope(spatiotemporalEnvelopeIterator.next());
                    while(spatiotemporalEnvelopeIterator.hasNext()) {
                        partitionEnvelope.expandToInclude(spatiotemporalEnvelopeIterator.next());
                    }
                    // assign the partition id
                    partitionEnvelope.id = index;
                    result.add(partitionEnvelope);
                    return result.iterator();
                }
            }
            , true).collect();
        this.partitioner = new CuboidPartitioner(cuboids);
    }
}
