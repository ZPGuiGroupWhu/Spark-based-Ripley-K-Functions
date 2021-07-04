package org.wysrc.crossK.spatiotemporalRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.wysrc.crossK.geom.SpatiotemporalEnvelope;
import org.wysrc.crossK.geom.SpatiotemporalPoint;

/**
 * The extended RDD for spatiotemporal point
 */
public class SpatiotemporalPointRDD extends SpatiotemporalRDD<SpatiotemporalPoint> {
    /**
     * Initiate a new spatiotemporal point RDD
     *
     * @param rawRDD the raw spatiotemporal RDD
     */
    public SpatiotemporalPointRDD(JavaRDD<SpatiotemporalPoint> rawRDD) {
        this.rawRDD = rawRDD;
    }

    /**
     * Initiate a new spatiotemporal point RDD and transform the CRS of spatial coordinates
     *
     * @param rawRDD the raw spatiotemporal RDD
     * @param sourceEPSGCode the source EPSG Code
     * @param targetEPSGCode the target EPSG Code
     */
    public SpatiotemporalPointRDD(JavaRDD<SpatiotemporalPoint> rawRDD, String sourceEPSGCode, String targetEPSGCode) {
        this.rawRDD = rawRDD;
        this.CRSTransform(sourceEPSGCode, targetEPSGCode);
    }

    /**
     * Initiate a new spatiotemporal point RDD, persist the raw RDD, and analyze its envelope and total count
     *
     * @param rawRDD the raw spatiotemporal RDD
     * @param storageLevel the storage level of raw RDD
     */
    public SpatiotemporalPointRDD(JavaRDD<SpatiotemporalPoint> rawRDD, StorageLevel storageLevel) {
        this.rawRDD = rawRDD;
        this.analyze(storageLevel);
    }

    /**
     * Initiate a new spatiotemporal point RDD, transform the CRS of spatial coordinates, persist the raw RDD,
     * and analyze its envelope and total count
     *
     * @param rawRDD the raw spatiotemporal RDD
     * @param sourceEPSGCode the source EPSG Code
     * @param targetEPSGCode the target EPSG Code
     * @param storageLevel the storage level of raw RDD
     */
    public SpatiotemporalPointRDD(JavaRDD<SpatiotemporalPoint> rawRDD, String sourceEPSGCode, String targetEPSGCode, StorageLevel storageLevel) {
        this.rawRDD = rawRDD;
        this.CRSTransform(sourceEPSGCode, targetEPSGCode);
        this.analyze(storageLevel);
    }

    /**
     * Initiate a new spatiotemporal point RDD, set its spatiotemporal envelope and total count
     *
     * @param rawRDD the raw spatiotemporal RDD
     * @param envelope the spatiotemporal envelope of the RDD
     * @param totalCount the total count of elements in the RDD
     */
    public SpatiotemporalPointRDD(JavaRDD<SpatiotemporalPoint> rawRDD, SpatiotemporalEnvelope envelope, long totalCount) {
        this.rawRDD = rawRDD;
        this.envelope = envelope;
        this.totalCount = totalCount;
    }

    /**
     * Initiate a new spatiotemporal point RDD, transform the CRS of spatial coordinates, set its spatiotemporal envelope and total count
     *
     * @param rawRDD the raw spatiotemporal RDD
     * @param sourceEPSGCode the source EPSG Code
     * @param targetEPSGCode the target EPSG Code
     * @param envelope the spatiotemporal envelope of the RDD
     * @param totalCount the total count of elements in the RDD
     */
    public SpatiotemporalPointRDD(JavaRDD<SpatiotemporalPoint> rawRDD, String sourceEPSGCode, String targetEPSGCode, SpatiotemporalEnvelope envelope, long totalCount) {
        this.rawRDD = rawRDD;
        this.CRSTransform(sourceEPSGCode, targetEPSGCode);
        this.envelope = envelope;
        this.totalCount = totalCount;
    }
}
