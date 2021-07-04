package org.wysrc.localK.geom;


import org.locationtech.jts.geom.*;
import org.locationtech.jts.util.Assert;

import java.time.LocalDateTime;
import java.time.temporal.TemporalUnit;

public class SpatiotemporalCircle extends SpatiotemporalGeometry {
    /**
     * Represents the center spatial coordinate of the spatiotemporal circle
     * (spatialGeometry inherited from SpatiotemporalGeometry represents the central spatial Geometry)
     */
    private Coordinate centerCoordinate;

    /**
     * Represents the radius of the circle in spatial dimension
     * (1. spatial radius must be greater than the radius of circumcircle of centerGeometry
     * 2. temporal radius is indirect represented in start time and end time, which is inherited from SpatiotemporalGeometry)
     */
    private double spatialRadius;

    /**
     * The radius of the circle in temporal dimension
     */
    private long temporalRadius;

    /**
     * The Unit for the temporal radius
     */
    private TemporalUnit temporalUnit;

    /**
     * Represents the center spatiotemporal geometry that constructs this spatiotemporal circle
     */
    private SpatiotemporalGeometry centerSpatiotemporalGeometry;

    public SpatiotemporalCircle(SpatiotemporalGeometry centerSpatiotemporalGeometry, double spatialRadius, long temporalRadius, TemporalUnit temporalUnit) {
        Assert.isTrue(spatialRadius >= 0 && temporalRadius >= 0);
        this.centerSpatiotemporalGeometry = centerSpatiotemporalGeometry;
        this.spatialGeometry = centerSpatiotemporalGeometry.getSpatialGeometry();
        Envelope centerGeometryEnvelope = this.spatialGeometry.getEnvelopeInternal();
        this.centerCoordinate = centerGeometryEnvelope.centre();
        double centerGeometryRadius = Math.sqrt(centerGeometryEnvelope.getWidth()*centerGeometryEnvelope.getWidth()+
                centerGeometryEnvelope.getHeight()*centerGeometryEnvelope.getHeight())/2;
        this.spatialRadius = spatialRadius>centerGeometryRadius? spatialRadius: centerGeometryRadius;
        this.temporalRadius = temporalRadius;
        this.temporalUnit = temporalUnit;
        this.startTime = centerSpatiotemporalGeometry.getStartTime().minus(temporalRadius, temporalUnit);
        this.endTime = centerSpatiotemporalGeometry.getEndTime().plus(temporalRadius, temporalUnit);
        this.envelope = new SpatiotemporalEnvelope(new Envelope(this.centerCoordinate.x-this.spatialRadius, this.centerCoordinate.x+this.spatialRadius,
                this.centerCoordinate.y-this.spatialRadius, this.centerCoordinate.y+this.spatialRadius), this.startTime, this.endTime);
    }

    public Coordinate getCenterCoordinate() {
        return this.centerCoordinate;
    }

    public double getSpatialRadius() {
        return this.spatialRadius;
    }

    public long getTemporalRadius() {
        return this.temporalRadius;
    }

    public TemporalUnit getTemporalUnit() {
        return this.temporalUnit;
    }

    public SpatiotemporalGeometry getCenterSpatiotemporalGeometry() {
        return this.centerSpatiotemporalGeometry;
    }

    public void setSpatialRadius(double spatialRadius) {
        Envelope centerGeometryEnvelope = this.spatialGeometry.getEnvelopeInternal();
        double centerGeometryRadius = Math.sqrt(centerGeometryEnvelope.getWidth()*centerGeometryEnvelope.getWidth()+
                centerGeometryEnvelope.getHeight()*centerGeometryEnvelope.getHeight())/2;
        this.spatialRadius = spatialRadius>centerGeometryRadius? spatialRadius: centerGeometryRadius;
        this.envelope = new SpatiotemporalEnvelope(new Envelope(this.centerCoordinate.x-this.spatialRadius, this.centerCoordinate.x+this.spatialRadius,
                this.centerCoordinate.y-this.spatialRadius, this.centerCoordinate.y+this.spatialRadius), this.startTime, this.endTime);
    }

    public void setTemporalRadius(long temporalRadius, TemporalUnit temporalUnit) {
        this.startTime = this.centerSpatiotemporalGeometry.getStartTime().minus(temporalRadius, temporalUnit);
        this.endTime = this.centerSpatiotemporalGeometry.getEndTime().plus(temporalRadius, temporalUnit);
        this.envelope = new SpatiotemporalEnvelope(new Envelope(this.centerCoordinate.x-this.spatialRadius, this.centerCoordinate.x+this.spatialRadius,
                this.centerCoordinate.y-this.spatialRadius, this.centerCoordinate.y+this.spatialRadius), this.startTime, this.endTime);
    }

    public boolean covers(SpatiotemporalGeometry other) {
        if(! getEnvelopeInternal().covers(other.getEnvelopeInternal())) {
            return false;
        }

        if(other instanceof SpatiotemporalPoint) {
            return covers((SpatiotemporalPoint) other);
        }

        if(other instanceof SpatiotemporalPolygon) {
            return covers((SpatiotemporalPolygon) other);
        }

        throw new IllegalArgumentException("SpatiotemporalCircle.covers() doesn't support SpatiotemporalGeometry Type: " +
                other.getSpatiotemporalGeometryType());
    }

    private boolean covers(SpatiotemporalPoint spatiotemporalPoint) {
        LocalDateTime startTimeOfPoint = spatiotemporalPoint.getStartTime();
        LocalDateTime endTimeOfPoint = spatiotemporalPoint.getEndTime();
        if(! (startTimeOfPoint.compareTo(this.startTime) >= 0 &&
                endTimeOfPoint.compareTo(this.endTime) <= 0)) {
            return false;
        }

        return covers((Point) spatiotemporalPoint.getSpatialGeometry());
    }

    private boolean covers(SpatiotemporalPolygon spatiotemporalPolygon) {
        LocalDateTime startTimeOfPolygon = spatiotemporalPolygon.getStartTime();
        LocalDateTime endTimeOfPolygon = spatiotemporalPolygon.getEndTime();
        if(! (startTimeOfPolygon.compareTo(this.startTime) >=0 &&
                endTimeOfPolygon.compareTo(this.endTime) <= 0)) {
            return false;
        }

        LineString lineString = ((Polygon) spatiotemporalPolygon.getSpatialGeometry()).getExteriorRing();
        for(int i=0; i<lineString.getNumPoints(); i++) {
            if(! covers(lineString.getPointN(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean covers(Point point) {
        double deltaX = point.getX() - centerCoordinate.x;
        double deltaY = point.getY() - centerCoordinate.y;
        return (deltaX*deltaX + deltaY*deltaY) <= spatialRadius*spatialRadius;
    }

    public boolean contains(SpatiotemporalGeometry other) {
        if(! getEnvelopeInternal().contains(other.getEnvelopeInternal())) {
            return false;
        }

        if(other instanceof SpatiotemporalPoint) {
            return contains((SpatiotemporalPoint) other);
        }

        throw new IllegalArgumentException("SpatiotemporalCircle.contains() doesn't support SpatiotemporalGeometry Type: " +
                other.getSpatiotemporalGeometryType());
    }

    private boolean contains(SpatiotemporalPoint spatiotemporalPoint) {
        LocalDateTime startTimeOfPoint = spatiotemporalPoint.getStartTime();
        LocalDateTime endTimeOfPoint = spatiotemporalPoint.getEndTime();
        if(! (startTimeOfPoint.compareTo(this.startTime) > 0 &&
                endTimeOfPoint.compareTo(this.endTime) < 0)) {
            return false;
        }

        return contains((Point) spatiotemporalPoint.getSpatialGeometry());
    }

    private boolean contains(Point point) {
        double deltaX = point.getX() - centerCoordinate.x;
        double deltaY = point.getY() - centerCoordinate.y;
        return (deltaX*deltaX + deltaY*deltaY) < spatialRadius*spatialRadius;
    }

    public boolean intersects(SpatiotemporalGeometry other) {
        if(! getEnvelopeInternal().intersects(other.getEnvelopeInternal())) {
            return false;
        }

        if(other instanceof SpatiotemporalPoint) {
            return covers((SpatiotemporalPoint) other);
        }

        if(other instanceof SpatiotemporalPolygon) {
            return intersects((SpatiotemporalPolygon) other);
        }

        throw new IllegalArgumentException("SpatiotemporalCircle.intersects() doesn't support SpatiotemporalGeometry Type: " +
                other.getSpatiotemporalGeometryType());
    }

    private boolean intersects(SpatiotemporalPolygon spatiotemporalPolygon) {
        LocalDateTime startTimeOfPolygon = spatiotemporalPolygon.getStartTime();
        LocalDateTime endTimeOfPolygon = spatiotemporalPolygon.getEndTime();
        if(startTimeOfPolygon.compareTo(this.endTime) > 0 ||
                endTimeOfPolygon.compareTo(this.startTime) < 0) {
            return false;
        }

        Polygon polygon = (Polygon) spatiotemporalPolygon.getSpatialGeometry();
        if(intersects(polygon.getExteriorRing())) {
            return true;
        }

        if(polygon.contains(polygon.getFactory().createPoint(centerCoordinate))) {
            return true;
        }

        for(int i=0; i<polygon.getNumInteriorRing(); i++) {
            if(intersects(polygon.getInteriorRingN(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean intersects(LineString lineString) {
        for(int i=0; i<lineString.getNumPoints()-1; i++) {
            if(intersects(lineString.getPointN(i), lineString.getPointN(i+1))) {
                return true;
            }
        }
        return false;
    }

    private boolean intersects(Point start, Point end) {
        double deltaX = end.getX() - start.getX();
        double deltaY = end.getY() - start.getY();

        double centerDeltaX = start.getX() - centerCoordinate.x;
        double centerDeltaY = start.getY() - centerCoordinate.y;

        double a = deltaX*deltaX + deltaY*deltaY;
        double b = 2 * (deltaX*centerDeltaX + deltaY*centerDeltaY);
        double c = centerDeltaX*centerDeltaX + centerDeltaY*centerDeltaY - spatialRadius*spatialRadius;

        double discriminant = b*b - 4*a*c;

        if(discriminant < 0) {
            return false;
        }

        double t1 = (-b + Math.sqrt(discriminant)) / (2*a);
        if(t1 >= 0 && t1 <= 1) {
            return true;
        }
        double t2 = (-b - Math.sqrt(discriminant)) / (2*a);
        if(t2 >= 0 && t2 <= 1) {
            return true;
        }

        return (Math.signum(t1) != Math.signum(t2));
    }

    @Override
    public String getSpatiotemporalGeometryType() {
        return "SpatiotemporalCircle";
    }

    @Override
    public boolean isEmpty() {
        return spatialGeometry.isEmpty() || startTime == null || endTime == null || spatialRadius <= 0;
    }

    @Override
    public Object clone() {
        SpatiotemporalGeometry cloneCenterGeometry = (SpatiotemporalGeometry) this.centerSpatiotemporalGeometry.clone();
        return new SpatiotemporalCircle(cloneCenterGeometry, this.spatialRadius, this.temporalRadius, this.temporalUnit);
    }

    @Override
    public boolean equals(Object o) {
        if(! (o instanceof SpatiotemporalCircle)) {
            return false;
        }
        SpatiotemporalCircle g = (SpatiotemporalCircle) o;
        return this.spatialGeometry.equals(g.spatialGeometry) &&
                this.spatialRadius == g.spatialRadius &&
                this.startTime.equals(g.startTime) &&
                this.endTime.equals(g.endTime);
    }

    @Override
    protected SpatiotemporalEnvelope computeEnvelopeInternal() {
        if(isEmpty()) {
            return new SpatiotemporalEnvelope();
        }
        return new SpatiotemporalEnvelope(new Envelope(this.centerCoordinate.x-this.spatialRadius, this.centerCoordinate.x+this.spatialRadius,
                this.centerCoordinate.y-this.spatialRadius, this.centerCoordinate.y+this.spatialRadius), startTime, endTime);
    }

    @Override
    protected int compareToSameClass(Object other) {
        SpatiotemporalCircle otherSpatiotemporalCircle = (SpatiotemporalCircle) other;
        return this.envelope.compareTo(otherSpatiotemporalCircle.envelope);
    }

    @Override
    public String toString() {
        return "Center: " + spatialGeometry.toString() + ", Radius: " + spatialRadius + ", Time (" + startTime.toString() + " " + endTime.toString() +")";
    }
}
