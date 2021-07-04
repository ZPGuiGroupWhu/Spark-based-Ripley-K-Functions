package org.wysrc.crossK.geom;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.util.Assert;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Iterator;

/**
 * A presentation of spatiotemporal geometry
 */
public abstract class SpatiotemporalGeometry implements Cloneable, Comparable, Serializable {

    int type;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    /**
     * A presentation of spatial geometry
     */
    Geometry spatialGeometry;

    /**
     * The timezone ID
     */
    ZoneId zoneId = ZoneId.systemDefault();

    /**
     * A presentation of start time of the geometry
     */
    LocalDateTime startTime;

    /**
     * A presentation of end time of the geometry
     */
    LocalDateTime endTime;

    /**
     * The spatiotemporal envelope of the geometry
     */
    protected SpatiotemporalEnvelope envelope;

    /**
     * Returns the name of this SpatiotemporalGeometry's actual class.
     *
     * @return the name of this SpatiotemporalGeometry's actual class
     */
    public abstract String getSpatiotemporalGeometryType();

    /**
     * Returns the spatial geometry.
     *
     * @return the spatial geometry
     */
    public Geometry getSpatialGeometry() {
        return this.spatialGeometry;
    }

    /**
     * Sets the spatial geometry.
     *
     * @param spatialGeometry the input spatial geometry
     */
    public void setSpatialGeometry(Geometry spatialGeometry) {
        this.spatialGeometry = spatialGeometry;
    }

    /**
     * Returns the zoneId of the spatiotemporalGeometry
     * @return the zoneId
     */
    public ZoneId getZoneId() {
        return this.zoneId;
    }

    /**
     * Returns the start time of the geometry.
     *
     * @return the start time of the geometry
     */
    public LocalDateTime getStartTime() {
        return this.startTime;
    }

    /**
     * Sets the start time.
     *
     * @param startTime the input start time
     */
    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    /**
     * Returns the end time of the geometry.
     *
     * @return the end time of the geometry
     */
    public LocalDateTime getEndTime() {
        return this.endTime;
    }

    /**
     * Sets the end time.
     *
     * @param endTime the input end time
     */
    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    /**
     * Gets the user data from spatial geometry.
     *
     * @return the user data
     */
    public Object getUserData() {
        return this.spatialGeometry.getUserData();
    }

    /**
     * Sets the user data for the geometry.
     *
     * @param userData input user data
     */
    public void setUserData(Object userData) {
        this.spatialGeometry.setUserData(userData);
    }

    /**
     * Tests whether the set of points covered by the spatiotemporal geometry is empty.
     *
     * @return true if this spatiotemporal geometry does not cover any points
     */
    public abstract boolean isEmpty();

    /**
     * Calculates the spatiotemporal envelope of the geometry
     *
     * @return the spatiotemporal envelope of the geometry
     */
    protected abstract SpatiotemporalEnvelope computeEnvelopeInternal();

    /**
     * Gets a copy of the spatiotemporal envelope that covers the spatiotemporal geometry.
     *
     * @return the envelope of the spatiotemporal geometry (when the geometry is empty, the envelope is empty too)
     */
    public SpatiotemporalEnvelope getEnvelopeInternal() {
        if(envelope == null) {
            envelope = computeEnvelopeInternal();
        }
        return new SpatiotemporalEnvelope(envelope);
    }

    /**
     * Transform the coordinates of geometry from source CRS to target CRS
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
            this.spatialGeometry = JTS.transform(this.spatialGeometry, transform);

            return true;
        } catch (FactoryException | TransformException e) {
            // TODO: more effective and proper catch
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Check if this geometry intersects the other geometry.
     *
     * @param other the geometry to check with
     * @return true if the geometries intersects
     */
    public boolean intersects(SpatiotemporalGeometry other) {
        if(isEmpty() || other.isEmpty()) {
            return false;
        }
        return this.spatialGeometry.intersects(other.spatialGeometry) &&
                !(other.startTime.compareTo(this.endTime) > 0 || other.endTime.compareTo(this.startTime) < 0);
    }

    /**
     * Check if this geometry covers the other geometry (including the boundary).
     *
     * @param other the geometry to check with
     * @return true if this geometry covers the other geometry
     */
    public boolean covers(SpatiotemporalGeometry other) {
        if(isEmpty() || other.isEmpty()) {
            return false;
        }
        return this.spatialGeometry.covers(other.spatialGeometry) &&
                other.startTime.compareTo(this.startTime) >= 0 &&
                other.endTime.compareTo(this.endTime) <= 0;
    }

    /**
     * Check if this geometry contains the other geometry (excluding the boundary).
     *
     * @param other the geometry to check with
     * @return true if this geometry contains the other geometry
     */
    public boolean contains(SpatiotemporalGeometry other) {
        if(isEmpty() || other.isEmpty()) {
            return false;
        }
        return this.spatialGeometry.contains(other.spatialGeometry) &&
                other.startTime.compareTo(this.startTime) > 0 &&
                other.endTime.compareTo(this.endTime) < 0;
    }

    /**
     * Tests whether this spatiotemporal geometry is structurally and numerically equal to a given Object.
     *
     * @param o the object to compare
     * @return true if this spatiotemporal geometry is exactly equal to the argument
     */
    @Override
    public boolean equals(Object o) {
        if(! (o instanceof SpatiotemporalGeometry)) {
            return false;
        }
        SpatiotemporalGeometry g = (SpatiotemporalGeometry) o;
        return this.spatialGeometry.equals(g.spatialGeometry) &&
                this.zoneId.equals(g.zoneId) &&
                this.startTime.equals(g.startTime) &&
                this.endTime.equals(g.endTime);
    }

    /**
     * Gets a hash code of the spatiotemporal geometry.
     * For best performance, the return value should be cached when it is accessed frequently.
     *
     * @return an integer value as hashcode
     */
    @Override
    public int hashCode() {
        return getEnvelopeInternal().hashCode();
    }

    /**
     * Creates and returns a full copy of this SpatiotemporalGeometry object
     * (including all internal data).
     * Subclass are responsible for overriding this method.
     *
     * @return a copy of this instance
     */
    @Override
    public Object clone() {
        try {
            SpatiotemporalGeometry clone = (SpatiotemporalGeometry) super.clone();
            // TODO: clone the internal data
            return clone;
        } catch (CloneNotSupportedException e) {
            Assert.shouldNeverReachHere();
            return null;
        }
    }

    /**
     * Compares this Geometry with another Geometry
     *
     * @param o a Geometry
     * @return a positive number, 0, or negative number, meaning this Geometry is greater than, equal to, or less than o
     */
    @Override
    public int compareTo(Object o) {
        SpatiotemporalGeometry other = (SpatiotemporalGeometry) o;
        // TODO: compare different classes of Geometry
        if(isEmpty() && other.isEmpty()) {
            return 0;
        }
        if(isEmpty()) {
            return -1;
        }
        if(other.isEmpty()) {
            return 1;
        }
        return compareToSameClass(o);
    }

    /**
     * Compares this Geometry with another Geometry having the same class
     *
     * @param o a Geometry having the same class as this Geometry
     * @return a positive number, 0, or negative number, meaning this Geometry is greater than, equal to, or less than o
     */
    protected abstract int compareToSameClass(Object o);

    /**
     * Compares two collection of Comparable
     *
     * @param a a collection of Comparable
     * @param b a collection of Comparable
     * @return the first non-zero compareTo result, if any; otherwise, zero
     */
    protected int compare(Collection a, Collection b) {
        Iterator i = a.iterator();
        Iterator j = b.iterator();
        while (i.hasNext() && j.hasNext()) {
            Comparable aElement = (Comparable) i.next();
            Comparable bElement = (Comparable) j.next();
            int comparison = aElement.compareTo(bElement);
            if (comparison != 0) {
                return comparison;
            }
        }
        if (i.hasNext()) {
            return 1;
        }
        if (j.hasNext()) {
            return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return spatialGeometry.toString() + ", Time (" + startTime.toString() + " " + endTime.toString() +")";
    }
}
