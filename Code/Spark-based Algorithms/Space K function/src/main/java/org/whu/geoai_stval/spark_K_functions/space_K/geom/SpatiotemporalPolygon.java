package org.whu.geoai_stval.spark_K_functions.space_K.geom;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;

public class SpatiotemporalPolygon extends SpatiotemporalGeometry {
    public SpatiotemporalPolygon(double[] coordinateValues, LocalDateTime time) {
        this(coordinateValues, time, time);
    }
    public double getArea() {
        return this.spatialGeometry.getArea();
    }
    public SpatiotemporalPolygon(double[] coordinateValues, LocalDateTime startTime, LocalDateTime endTime) {
        Coordinate[] vertexes = new Coordinate[coordinateValues.length/2];
        for(int i=0; i<coordinateValues.length/2; i++) {
            vertexes[i] = new Coordinate(coordinateValues[i*2], coordinateValues[i*2+1]);
        }
        GeometryFactory geometryFactory = new GeometryFactory();
        this.spatialGeometry = geometryFactory.createPolygon(vertexes);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public SpatiotemporalPolygon(double[] coordinateValues, LocalDateTime startTime, LocalDateTime endTime,
                                 String sourceEPSGCode, String targetEPSGCode, boolean longitudeFirst) {
        Coordinate[] vertexes = new Coordinate[coordinateValues.length/2];
        if(longitudeFirst) {
            for(int i=0; i<coordinateValues.length/2; i++) {
                vertexes[i] = new Coordinate(coordinateValues[i*2+1], coordinateValues[i*2]);
            }
        } else {
            for(int i=0; i<coordinateValues.length/2; i++) {
                vertexes[i] = new Coordinate(coordinateValues[i*2], coordinateValues[i*2+1]);
            }
        }
        GeometryFactory geometryFactory = new GeometryFactory();
        this.spatialGeometry = geometryFactory.createPolygon(vertexes);
        CRSTransform(sourceEPSGCode, targetEPSGCode);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public SpatiotemporalPolygon(Polygon spatialPolygon, LocalDateTime startTime, LocalDateTime endTime) {
        this.spatialGeometry = spatialPolygon;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public SpatiotemporalPolygon(SpatiotemporalEnvelope spatiotemporalEnvelope) {
        Envelope spatialEnvelope = spatiotemporalEnvelope.spatialEnvelope;
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate[] vertexes = new Coordinate[5];
        vertexes[0] = new Coordinate(spatialEnvelope.getMinX(), spatialEnvelope.getMinY());
        vertexes[1] = new Coordinate(spatialEnvelope.getMinX(), spatialEnvelope.getMaxY());
        vertexes[2] = new Coordinate(spatialEnvelope.getMaxX(), spatialEnvelope.getMaxY());
        vertexes[3] = new Coordinate(spatialEnvelope.getMaxX(), spatialEnvelope.getMinY());
        vertexes[4] = new Coordinate(spatialEnvelope.getMinX(), spatialEnvelope.getMinY());
        this.spatialGeometry = geometryFactory.createPolygon(vertexes);
        this.startTime = spatiotemporalEnvelope.startTime;
        this.endTime = spatiotemporalEnvelope.endTime;
    }

    public double getVolume(TemporalUnit temporalUnit) {
        if(temporalUnit instanceof ChronoUnit) {
            ChronoUnit chronoUnit = (ChronoUnit) temporalUnit;
            switch (chronoUnit) {
                // if startTime equals endTime, return spatial area
                case YEARS: {
                    long temporalInterval = Math.abs(ChronoUnit.YEARS.between(startTime.toLocalDate(), endTime.toLocalDate()));
                    if(temporalInterval == 0) {
                        return this.spatialGeometry.getArea();
                    } else {
                        return temporalInterval * this.spatialGeometry.getArea();
                    }
                }
                case MONTHS: {
                    long temporalInterval = Math.abs(ChronoUnit.MONTHS.between(startTime.toLocalDate(), endTime.toLocalDate()));
                    if(temporalInterval == 0) {
                        return this.spatialGeometry.getArea();
                    } else {
                        return temporalInterval * this.spatialGeometry.getArea();
                    }
                }
                case DAYS: {
                    long temporalInterval = Math.abs(ChronoUnit.DAYS.between(startTime.toLocalDate(), endTime.toLocalDate()));
                    if(temporalInterval == 0) {
                        return this.spatialGeometry.getArea();
                    } else {
                        return temporalInterval * this.spatialGeometry.getArea();
                    }
                }
                case HOURS: {
                    long temporalInterval = Duration.between(startTime, endTime).abs().toHours();
                    if(temporalInterval == 0) {
                        return this.spatialGeometry.getArea();
                    } else {
                        return temporalInterval * this.spatialGeometry.getArea();
                    }
                }
                case MINUTES: {
                    long temporalInterval = Duration.between(startTime, endTime).abs().toMinutes();
                    if(temporalInterval == 0) {
                        return this.spatialGeometry.getArea();
                    } else {
                        return temporalInterval * this.spatialGeometry.getArea();
                    }
                }
                case SECONDS: {
                    long temporalInterval = Duration.between(startTime, endTime).abs().getSeconds();
                    if(temporalInterval == 0) {
                        return this.spatialGeometry.getArea();
                    } else {
                        return temporalInterval * this.spatialGeometry.getArea();
                    }
                }
            }
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + temporalUnit);
        }
        return temporalUnit.between(startTime, endTime) * this.spatialGeometry.getArea();
    }

    @Override
    public String getSpatiotemporalGeometryType() {
        return "SpatiotemporalPolygon";
    }

    @Override
    public boolean isEmpty() {
        Polygon spatialPolygon = (Polygon) spatialGeometry;
        return spatialPolygon.isEmpty() || this.startTime == null || this.endTime == null;
    }

    @Override
    public Object clone() {
        Polygon clonePolygon = (Polygon) this.spatialGeometry.clone();
        return new SpatiotemporalPolygon(clonePolygon, startTime, endTime);
    }

    @Override
    protected SpatiotemporalEnvelope computeEnvelopeInternal() {
        if(isEmpty()) {
            return new SpatiotemporalEnvelope();
        }
        Polygon spatialPolygon = (Polygon) spatialGeometry;
        return new SpatiotemporalEnvelope(spatialPolygon.getEnvelopeInternal(), startTime, endTime);
    }

    @Override
    protected int compareToSameClass(Object other) {
        Polygon spatialPolygon = (Polygon) spatialGeometry;
        SpatiotemporalPolygon otherSpatiotemporalPolygon = (SpatiotemporalPolygon) other;
        Polygon otherSpatialPolygon = (Polygon) otherSpatiotemporalPolygon.spatialGeometry;
        int spatialCompare = spatialPolygon.compareTo(otherSpatialPolygon);
        int startTimeCompare = this.startTime.compareTo(otherSpatiotemporalPolygon.startTime);
        int endTimeCompare = this.endTime.compareTo(otherSpatiotemporalPolygon.endTime);

        return spatialCompare!=0? spatialCompare: (startTimeCompare!=0? startTimeCompare: endTimeCompare);
    }
}
