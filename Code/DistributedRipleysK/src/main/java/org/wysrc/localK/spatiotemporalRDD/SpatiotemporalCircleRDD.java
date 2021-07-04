package org.wysrc.localK.spatiotemporalRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.wysrc.localK.geom.SpatiotemporalCircle;
import org.wysrc.localK.geom.SpatiotemporalGeometry;
import org.wysrc.localK.geom.SpatiotemporalPoint;

import java.time.temporal.TemporalUnit;

/**
 * The extended RDD for spatiotemporal circle
 */
public class SpatiotemporalCircleRDD extends SpatiotemporalRDD<SpatiotemporalCircle> {
    /**
     * Initiate a new SpatiotemporalCircleRDD
     *
     * @param rawRDD the raw SpatiotemporalCircle RDD
     */
    public SpatiotemporalCircleRDD(JavaRDD<SpatiotemporalCircle> rawRDD) {
        this.rawRDD = rawRDD;
    }

    /**
     * Initiate a new SpatiotemporalCircleRDD and transform the CRS of spatial coordinates
     *
     * @param rawRDD the raw SpatiotemporalCircle RDD
     * @param sourceEPSGCode the source EPSG Code
     * @param targetEPSGCode the target EPSG Code
     */
    public SpatiotemporalCircleRDD(JavaRDD<SpatiotemporalCircle> rawRDD, String sourceEPSGCode, String targetEPSGCode) {
        this.rawRDD = rawRDD;
        this.CRSTransform(sourceEPSGCode, targetEPSGCode);
    }

    /**
     * Initiate a new SpatiotemporalCircleRDD with center SpatiotemporalRDD, spatial radius and temporal radius
     *
     * @param spatiotemporalRDD the center SpatiotemporalRDD
     * @param spatialRadius the spatial radius
     * @param temporalRadius the temporal radius
     * @param temporalUnit the temporal unit for radius
     */
    public <T extends SpatiotemporalGeometry> SpatiotemporalCircleRDD(SpatiotemporalRDD<T> spatiotemporalRDD, double spatialRadius, long temporalRadius, TemporalUnit temporalUnit) {
        if(spatiotemporalRDD.partitionedRDD != null) {
            this.rawRDD = spatiotemporalRDD.partitionedRDD.map(new Function<T, SpatiotemporalCircle>() {
                @Override
                public SpatiotemporalCircle call(T spatiotemporalGeometry) {
                    return new SpatiotemporalCircle(spatiotemporalGeometry, spatialRadius, temporalRadius, temporalUnit);
                }
            });
        } else {
            this.rawRDD = spatiotemporalRDD.rawRDD.map(new Function<T, SpatiotemporalCircle>() {
                @Override
                public SpatiotemporalCircle call(T spatiotemporalGeometry) {
                    return new SpatiotemporalCircle(spatiotemporalGeometry, spatialRadius, temporalRadius, temporalUnit);
                }
            });
        }

        this.CRSTransformed = spatiotemporalRDD.CRSTransformed;
        this.sourceEPSGCode = spatiotemporalRDD.sourceEPSGCode;
        this.targetEPSGCode = spatiotemporalRDD.targetEPSGCode;
    }

    /**
     * Gets the center SpatiotemporalPointRDD
     *
     * @return the center SpatiotemporalPointRDD
     */
    public SpatiotemporalPointRDD getCenterPointAsRDD() {
        return new SpatiotemporalPointRDD(this.rawRDD.map(new Function<SpatiotemporalCircle, SpatiotemporalPoint>() {
            @Override
            public SpatiotemporalPoint call(SpatiotemporalCircle spatiotemporalCircle) {
              return new SpatiotemporalPoint(spatiotemporalCircle.getCenterCoordinate(), spatiotemporalCircle.getStartTime(), spatiotemporalCircle.getEndTime());
            }
        }));
    }
}
