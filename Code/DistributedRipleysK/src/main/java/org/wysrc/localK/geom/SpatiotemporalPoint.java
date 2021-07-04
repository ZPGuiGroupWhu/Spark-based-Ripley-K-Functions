package org.wysrc.localK.geom;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.time.LocalDateTime;

/**
 * A presentation of spatiotemporal point
 */
public class SpatiotemporalPoint extends SpatiotemporalGeometry {
    public SpatiotemporalPoint(Coordinate coordinate, LocalDateTime time) {
        this(coordinate, time, time);
    }

    public SpatiotemporalPoint(Coordinate coordinate, LocalDateTime startTime, LocalDateTime endTime) {
        GeometryFactory geometryFactory = new GeometryFactory();
        this.spatialGeometry = geometryFactory.createPoint(coordinate);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public SpatiotemporalPoint(Point spatialPoint, LocalDateTime time) {
        this(spatialPoint, time, time);
    }

    public SpatiotemporalPoint(Point spatialPoint, LocalDateTime startTime, LocalDateTime endTime) {
        this.spatialGeometry = spatialPoint;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String getSpatiotemporalGeometryType() {
        return "SpatiotemporalPoint";
    }

    @Override
    public boolean isEmpty() {
        Point spatialPoint = (Point) spatialGeometry;
        return spatialPoint.isEmpty() || this.startTime == null || this.endTime == null;
    }

    @Override
    public Object clone() {
        Point clonePoint = (Point) this.spatialGeometry.clone();
        return new SpatiotemporalPoint(clonePoint, this.startTime, this.endTime);
    }

    @Override
    protected SpatiotemporalEnvelope computeEnvelopeInternal() {
        if(isEmpty()) {
            return new SpatiotemporalEnvelope();
        }
        Point spatialPoint = (Point) spatialGeometry;
        return new SpatiotemporalEnvelope(spatialPoint.getEnvelopeInternal(), startTime, endTime);
    }

    @Override
    protected int compareToSameClass(Object other) {
        Point spatialPoint = (Point) spatialGeometry;
        SpatiotemporalPoint otherSpatiotemporalPoint = (SpatiotemporalPoint) other;
        Point otherSpatialPoint = (Point) otherSpatiotemporalPoint.spatialGeometry;
        int spatialCompare = spatialPoint.compareTo(otherSpatialPoint);
        int startTimeCompare = this.startTime.compareTo(otherSpatiotemporalPoint.startTime);
        int endTimeCompare = this.endTime.compareTo(otherSpatiotemporalPoint.endTime);

        return spatialCompare!=0? spatialCompare: (startTimeCompare!=0? startTimeCompare: endTimeCompare);
    }

    public String toSpatialString() {
        Point point = (Point) spatialGeometry;
        return point.getX() + "," + point.getY();
    }

    public String toTemporalString() {
        return startTime.toString();
    }
}
