package org.wysrc.distributedRipleysK.geom;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.wysrc.distributedRipleysK.utils.TimeUtils;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;

/**
 * A presentation of spatiotemporal envelope
 */
public class SpatiotemporalEnvelope implements Comparable, Serializable {
    /**
     * A presentation of spatial envelope
     */
    public Envelope spatialEnvelope;

    /**
     * The timezone ID
     */
    public ZoneId zoneId = ZoneId.systemDefault();

    /**
     * The start of temporal envelope
     */
    public LocalDateTime startTime;

    /**
     * The end of temporal envelope
     */
    public LocalDateTime endTime;

    /**
     * The identifier of the spatiotemporal envelope
     */
    public int id = -1;

    /**
     * Creates a null spatiotemporal envelope
     */
    public SpatiotemporalEnvelope() {
        init();
    }

    /**
     * Creates a spatiotemporal envelope defined by maximum and minimum values of x, y coordinates and date times.
     *
     * @param x1 the first x coordinate
     * @param x2 the second x coordinate
     * @param y1 the first y coordinate
     * @param y2 the second y coordinate
     * @param dateTime1 the first date time string
     * @param dateTime2 the second date time string
     * @param dateTimeFormatter the date time formatter
     */
    public SpatiotemporalEnvelope(double x1, double x2, double y1, double y2, String dateTime1, String dateTime2, DateTimeFormatter dateTimeFormatter) {
        init(x1, x2, y1, y2, dateTime1, dateTime2, dateTimeFormatter);
    }

    /**
     * Creates a spatiotemporal envelope defined by two Coordinates and date times.
     *
     * @param p1 the first Coordinate
     * @param p2 the second Coordinate
     * @param dateTime1 the first date time string
     * @param dateTime2 the second date time string
     * @param dateTimeFormatter the date time formatter
     */
    public SpatiotemporalEnvelope(Coordinate p1, Coordinate p2, String dateTime1, String dateTime2, DateTimeFormatter dateTimeFormatter) {
        init(p1, p2, dateTime1, dateTime2, dateTimeFormatter);
    }

    /**
     * Creates a spatiotemporal envelope defined by a single Coordinate and date time.
     *
     * @param p the Coordinate
     * @param dateTime the date time string
     * @param dateTimeFormatter the date time formatter
     */
    public SpatiotemporalEnvelope(Coordinate p, String dateTime, DateTimeFormatter dateTimeFormatter) {
        init(p, dateTime, dateTimeFormatter);
    }

    /**
     * Creates a spatiotemporal envelope from existing spatial envelope and two date time.
     *
     * @param spatialEnvelope existing spatial envelope
     * @param startTime existing start date time
     * @param endTime existing end date time
     */
    public SpatiotemporalEnvelope(Envelope spatialEnvelope, LocalDateTime startTime, LocalDateTime endTime) {
        init(spatialEnvelope, startTime, endTime);
    }

    /**
     * Creates a spatiotemporal envelope from existing spatial envelope and date time.
     *
     * @param spatialEnvelope existing spatial envelope
     * @param localDateTime existing date time
     */
    public SpatiotemporalEnvelope(Envelope spatialEnvelope, LocalDateTime localDateTime) {
        init(spatialEnvelope, localDateTime, localDateTime);
    }

    /**
     * Creates a spatiotemporal envelope from an existing envelope.
     *
     * @param envelope the envelope to initialize from
     */
    public SpatiotemporalEnvelope(SpatiotemporalEnvelope envelope) {
        init(envelope);
    }

    /**
     * Initialize to a null spatiotemporal envelope
     */
    public void init() {
        setToNull();
    }

    /**
     * Initialize a spatiotemporal envelope defined by maximum and minimum values of x, y coordinates and date times.
     *
     * @param x1 the first x coordinate
     * @param x2 the second x coordinate
     * @param y1 the first y coordinate
     * @param y2 the second y coordinate
     * @param dateTime1 the first date time string
     * @param dateTime2 the second date time string
     * @param dateTimeFormatter the date time formatter
     */
    public void init(double x1, double x2, double y1, double y2, String dateTime1, String dateTime2, DateTimeFormatter dateTimeFormatter) {
        this.spatialEnvelope = new Envelope(x1, x2, y1, y2);
        LocalDateTime localDateTime1 = dateTime1!=null && !dateTime1.isEmpty()? LocalDateTime.parse(dateTime1, dateTimeFormatter): LocalDateTime.now();
        LocalDateTime localDateTime2 = dateTime2!=null && !dateTime2.isEmpty()? LocalDateTime.parse(dateTime2, dateTimeFormatter): LocalDateTime.now();
        if(localDateTime1.compareTo(localDateTime2) < 0) {
            this.startTime = localDateTime1;
            this.endTime = localDateTime2;
        } else {
            this.startTime = localDateTime2;
            this.endTime = localDateTime1;
        }
    }

    /**
     * Initialize a spatiotemporal envelope defined by two Coordinates and date times.
     *
     * @param p1 the first Coordinate
     * @param p2 the second Coordinate
     * @param dateTime1 the first date time string
     * @param dateTime2 the second date time string
     * @param dateTimeFormatter the date time formatter
     */
    public void init(Coordinate p1, Coordinate p2, String dateTime1, String dateTime2, DateTimeFormatter dateTimeFormatter) {
        init(p1.x, p2.x, p1.y, p2.y, dateTime1, dateTime2, dateTimeFormatter);
    }

    /**
     * Initialize a spatiotemporal envelope defined by a single Coordinate and date time.
     *
     * @param p the Coordinate
     * @param dateTime the date time string
     * @param dateTimeFormatter the date time formatter
     */
    public void init(Coordinate p, String dateTime, DateTimeFormatter dateTimeFormatter) {
        init(p.x, p.x, p.y, p.y, dateTime, dateTime, dateTimeFormatter);
    }

    /**
     * Initialize a spatiotemporal envelope from existing spatial envelope and two date time.
     *
     * @param spatialEnvelope existing spatial envelope
     * @param startTime existing start date time
     * @param endTime existing end date time
     */
    public void init(Envelope spatialEnvelope, LocalDateTime startTime, LocalDateTime endTime) {
        this.spatialEnvelope = new Envelope(spatialEnvelope);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    /**
     * Initialize a spatiotemporal envelope from an existing envelope.
     *
     * @param envelope the envelope to initialize from
     */
    public void init(SpatiotemporalEnvelope envelope) {
        if(envelope != null && !envelope.isNull()) {
            this.spatialEnvelope = new Envelope(envelope.spatialEnvelope);
            this.startTime = envelope.startTime;
            this.endTime = envelope.endTime;
            this.zoneId = envelope.zoneId;
            this.id = envelope.id;
        }
    }

    /**
     * Makes this spatiotemporal envelope a null envelope
     */
    public void setToNull() {
        this.spatialEnvelope = new Envelope();
        this.startTime = LocalDateTime.MAX;
        this.endTime = LocalDateTime.MIN;
    }

    /**
     * Gets the hash code composed of hash codes from spatial envelope and temporal interval.
     *
     * @return the hash code of the spatiotemporal envelope
     */
    public int hashCode() {
        int spatialHashCode = spatialEnvelope.hashCode();
        int startTimeHashCode = startTime.hashCode();
        int endTimeHashCode = endTime.hashCode();
        return spatialHashCode ^ (startTimeHashCode ^ endTimeHashCode);
    }

    /**
     * Tests whether this spatiotemporal envelope is structurally and numerically equal to a given Object.
     *
     * @param other the object to compare
     * @return true if this spatiotemporal envelope is exactly equal to the argument
     */
    public boolean equals(Object other) {
        if(! (other instanceof SpatiotemporalEnvelope)) {
            return false;
        }
        SpatiotemporalEnvelope otherEnvelope = (SpatiotemporalEnvelope) other;
        if(isNull()) {
            return otherEnvelope.isNull();
        }
        return this.spatialEnvelope.equals(otherEnvelope.spatialEnvelope) &&
                this.startTime.equals(otherEnvelope.startTime) &&
                this.endTime.equals(otherEnvelope.endTime);
    }

    /**
     * Tests whether the spatiotemporal envelope is a "null" envelope.
     *
     * @return true if this envelope is uninitialized or is the envelope of an empty geometry.
     */
    public boolean isNull() {
        if(this.spatialEnvelope == null || this.spatialEnvelope.isNull()) {
            return true;
        }
        // if start time is greater than end time, the temporal envelope is null
        return this.startTime.compareTo(this.endTime) > 0;
    }

    /**
     * Gets the spatial area of the spatiotemporal envelope
     *
     * @return the spatial area
     */
    public double getArea() {
        return this.spatialEnvelope.getArea();
    }

    /**
     * Gets the spatial width of the spatiotemporal envelope
     *
     * @return the width
     */
    public double getWidth() {
        return this.spatialEnvelope.getWidth();
    }

    /**
     * Gets the spatial height of the spatiotemporal envelope
     *
     * @return the height
     */
    public double getHeight() {
        return this.spatialEnvelope.getHeight();
    }

    /**
     * Gets the temporal interval of the spatiotemporal envelope with default unit SECONDS
     *
     * @return the temporal interval
     */
    public double getTemporalInterval() {
        return getTemporalInterval(ChronoUnit.SECONDS);
    }

    /**
     * Gets the temporal interval of the spatiotemporal envelope
     *
     * @param temporalUnit the temporal unit to compute the interval
     * @return the temporal interval
     */
    public long getTemporalInterval(TemporalUnit temporalUnit) {
        return TimeUtils.calTemporalDistance(startTime, endTime, temporalUnit);
    }

    /**
     * Gets the volume of the spatiotemporal envelope with default temporal unit SECONDS
     *
     * @return the volume
     */
    public double getVolume() {
        return getVolume(ChronoUnit.SECONDS);
    }

    /**
     * Gets the volume of the spatiotemporal envelope
     *
     * @param temporalUnit the temporal unit to compute the volume
     * @return the volume
     */
    public double getVolume(TemporalUnit temporalUnit) {
        double area = this.spatialEnvelope.getArea();
        long temporalInternal = getTemporalInterval(temporalUnit);
        return temporalInternal * area;
    }

    /**
     * Enlarges this spatiotemporal envelope so that it contains the given point.
     * Has no effect if the point is already on or within the envelope.
     *
     * @param point the spatiotemporal point to expand to include
     */
    public void expandToInclude(SpatiotemporalPoint point) {
        if(point.isEmpty()) {
            return;
        }
        SpatiotemporalEnvelope envelope = point.getEnvelopeInternal();
        this.expandToInclude(envelope);
    }

    /**
     * Enlarges this spatiotemporal envelope so that it contains the other envelope.
     * Has no effect if the other envelope is wholly on or within the envelope.
     *
     * @param envelope the spatiotemporal envelope to expand to include
     */
    public void expandToInclude(SpatiotemporalEnvelope envelope) {
        if(envelope.isNull()) {
            return;
        }
        if(isNull()) {
            this.spatialEnvelope = new Envelope(envelope.spatialEnvelope);
            this.startTime = envelope.startTime;
            this.endTime = envelope.endTime;
        } else {
            this.spatialEnvelope.expandToInclude(envelope.spatialEnvelope);
            if(envelope.startTime.compareTo(this.startTime) < 0) {
                this.startTime = envelope.startTime;
            }
            if(envelope.endTime.compareTo(this.endTime) > 0) {
                this.endTime = envelope.endTime;
            }
        }
    }

    public SpatiotemporalPoint center() {
        if(isNull()) {
            return null;
        }
        long duration = Duration.between(startTime, endTime).abs().getSeconds();
        LocalDateTime centerTime = startTime.plus(Duration.ofSeconds(duration/2));
        return new SpatiotemporalPoint(this.spatialEnvelope.centre(), centerTime);
    }

    /**
     * Computes the intersection of two spatiotemporal envelope.
     *
     * @param envelope the envelope to intersect with
     * @return a new spatiotemporal envelope representing the intersection of the envelopes
     */
    public SpatiotemporalEnvelope intersection(SpatiotemporalEnvelope envelope) {
        if(isNull() || envelope.isNull() || !this.intersects(envelope)) {
            return new SpatiotemporalEnvelope();
        }
        Envelope spatialEnvelope = this.spatialEnvelope.intersection(envelope.spatialEnvelope);
        LocalDateTime startTime = this.startTime.compareTo(envelope.startTime) > 0? this.startTime: envelope.startTime;
        LocalDateTime endTime = this.endTime.compareTo(envelope.endTime) < 0? this.endTime: envelope.endTime;
        return new SpatiotemporalEnvelope(spatialEnvelope, startTime, endTime);
    }

    /**
     * Check if this envelope intersects the other envelope.
     *
     * @param other the envelope to check with
     * @return true if the envelopes intersects
     */
    public boolean intersects(SpatiotemporalEnvelope other) {
        if(isNull() || other.isNull()) {
            return false;
        }
        return this.spatialEnvelope.intersects(other.spatialEnvelope) &&
                !(other.startTime.compareTo(this.endTime) > 0 || other.endTime.compareTo(this.startTime) < 0);
    }

    /**
     * Check if this envelope covers the other envelope (including the boundary).
     *
     * @param other the envelope to check with
     * @return true if this envelope covers the other envelope
     */
    public boolean covers(SpatiotemporalEnvelope other) {
        if(isNull() || other.isNull()) {
            return false;
        }
        return this.spatialEnvelope.covers(other.spatialEnvelope) &&
                other.startTime.compareTo(this.startTime) >= 0 &&
                other.endTime.compareTo(this.endTime) <= 0;
    }

    /**
     * Check if this envelope contains the other envelope (excluding the boundary).
     *
     * @param other the envelope to check with
     * @return true if this envelope contains the other envelope
     */
    public boolean contains(SpatiotemporalEnvelope other) {
        if(isNull() || other.isNull()) {
            return false;
        }
        return other.spatialEnvelope.getMinX() > this.spatialEnvelope.getMinX() &&
                other.spatialEnvelope.getMaxX() < this.spatialEnvelope.getMaxX() &&
                other.spatialEnvelope.getMinY() > this.spatialEnvelope.getMinY() &&
                other.spatialEnvelope.getMaxY() < this.spatialEnvelope.getMaxY() &&
                other.startTime.compareTo(this.startTime) > 0 &&
                other.endTime.compareTo(this.endTime) < 0;
    }

    /**
     * Check if this envelope is adjacent to the other envelope.
     * (Be equal at two dimensions, and be adjacent at the other dimension.)
     *
     * @param other the envelope to check with
     * @return true if this envelope is adjacent to the other envelope
     */
    public boolean isAdjacentTo(SpatiotemporalEnvelope other) {
        if(isNull() || other.isNull()) {
            return false;
        }
        boolean isAdjacentAtX = this.spatialEnvelope.getMinX() == other.spatialEnvelope.getMaxX() ||
                this.spatialEnvelope.getMaxX() == other.spatialEnvelope.getMinX();
        boolean isAdjacentAtY = this.spatialEnvelope.getMinY() == other.spatialEnvelope.getMaxY() ||
                this.spatialEnvelope.getMaxY() == other.spatialEnvelope.getMinY();
        boolean isAdjacentAtTime = this.startTime.equals(other.endTime) || this.endTime.equals(other.startTime);
        boolean isEqualAtXAndY = this.spatialEnvelope.getMinX() == other.spatialEnvelope.getMinX() &&
                this.spatialEnvelope.getMaxX() == other.spatialEnvelope.getMaxX() &&
                this.spatialEnvelope.getMinY() == other.spatialEnvelope.getMinY() &&
                this.spatialEnvelope.getMaxY() == other.spatialEnvelope.getMaxY();
        boolean isEqualAtXAndTime = this.spatialEnvelope.getMinX() == other.spatialEnvelope.getMinX() &&
                this.spatialEnvelope.getMaxX() == other.spatialEnvelope.getMaxX() &&
                this.startTime.equals(other.startTime) && this.endTime.equals(other.endTime);
        boolean isEqualAtYAndTime = this.spatialEnvelope.getMinY() == other.spatialEnvelope.getMinY() &&
                this.spatialEnvelope.getMaxY() == other.spatialEnvelope.getMaxY() &&
                this.startTime.equals(other.startTime) && this.endTime.equals(other.endTime);
        return (isAdjacentAtX && isEqualAtYAndTime) || (isAdjacentAtY && isEqualAtXAndTime) || (isAdjacentAtTime && isEqualAtXAndY);
    }

    /**
     * Compares two spatiotemporal envelopes using spatial-temporal ordering.
     * Null envelopes are less than all non-null envelopes.
     *
     * @param o the spatiotemporal envelope to compare
     * @return a positive number, 0, or negative number, meaning this envelope is greater than, equal to, or less than o.
     */
    public int compareTo(Object o) {
        SpatiotemporalEnvelope spatiotemporalEnvelope = (SpatiotemporalEnvelope) o;
        // compare null if present
        if(isNull()) {
            if(spatiotemporalEnvelope.isNull()) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if(spatiotemporalEnvelope.isNull()) {
                return 1;
            }
        }

        // compare based on spatial dimension
        int spatialCompare = this.spatialEnvelope.compareTo(spatiotemporalEnvelope.spatialEnvelope);
        if(spatialCompare != 0) {
            return spatialCompare;
        }

        // compare based on temporal dimension
        if(this.startTime.compareTo(spatiotemporalEnvelope.startTime) < 0) { return -1; }
        if(this.startTime.compareTo(spatiotemporalEnvelope.startTime) > 0) { return 1; }
        if(this.endTime.compareTo(spatiotemporalEnvelope.endTime) < 0) { return -1; }
        if(this.endTime.compareTo(spatiotemporalEnvelope.endTime) > 0) { return 1; }

        return 0;
    }
}
