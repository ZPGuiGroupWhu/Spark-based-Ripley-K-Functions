package org.wysrc.spatialK.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
import org.wysrc.spatialK.geom.SpatiotemporalCircle;
import org.wysrc.spatialK.geom.SpatiotemporalEnvelope;
import org.wysrc.spatialK.geom.SpatiotemporalGeometry;
import org.wysrc.spatialK.geom.SpatiotemporalPoint;
import org.wysrc.spatialK.utils.TimeUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

public class SpatiotemporalGeometrySerde extends Serializer {
    private static final Logger log = Logger.getLogger(SpatiotemporalGeometrySerde.class);
    private static final GeometryFactory geometryFactory = new GeometryFactory();

    private enum SpatiotemporalGeometryType {
        SPATIOTEMPORAL_POINT(0),
        SPATIOTEMPORAL_CIRCLE(1),
        SPATIOTEMPORAL_ENVELOPE(2);

        private final int id;

        SpatiotemporalGeometryType(int id) {
            this.id = id;
        }

        public static SpatiotemporalGeometryType fromId(int id) {
            for(SpatiotemporalGeometryType type: values()) {
                if(type.id == id) {
                    return type;
                }
            }
            return null;
        }
    }

    @Override
    public void write(Kryo kryo, Output out, Object object) {
        if(object instanceof SpatiotemporalPoint) {
            writeType(out, SpatiotemporalGeometryType.SPATIOTEMPORAL_POINT);
            writeSpatiotemporalGeometry(kryo, out, (SpatiotemporalGeometry) object);
        } else if(object instanceof SpatiotemporalCircle) {
            writeType(out, SpatiotemporalGeometryType.SPATIOTEMPORAL_CIRCLE);
            SpatiotemporalCircle circle = (SpatiotemporalCircle) object;
            SpatiotemporalGeometry centerGeometry = circle.getCenterSpatiotemporalGeometry();
            double spatialRadius = circle.getSpatialRadius();
            long temporalRadius = circle.getTemporalRadius();
            TemporalUnit temporalUnit = circle.getTemporalUnit();
            writeType(out, SpatiotemporalGeometryType.SPATIOTEMPORAL_POINT);
            writeSpatiotemporalGeometry(kryo, out, centerGeometry);
            out.writeDouble(spatialRadius);
            out.writeLong(temporalRadius);
            writeTemporalUnit(out, temporalUnit);
        } else if(object instanceof SpatiotemporalEnvelope) {
            writeType(out, SpatiotemporalGeometryType.SPATIOTEMPORAL_ENVELOPE);
            SpatiotemporalEnvelope envelope = (SpatiotemporalEnvelope) object;
            Envelope spatialEnvelope = envelope.spatialEnvelope;
            ZoneId zoneId = envelope.zoneId;
            LocalDateTime startTime = envelope.startTime;
            LocalDateTime endTime = envelope.endTime;
            int id = envelope.id;
            writeEnvelope(out, spatialEnvelope);
            writeTime(out, startTime, zoneId);
            writeTime(out, endTime, zoneId);
            out.writeInt(id);
        } else {
            throw new UnsupportedOperationException("Cannot serialize object of type: " + object.getClass().getName());
        }
    }

    private void writeType(Output out, SpatiotemporalGeometryType type) {
        out.writeByte((byte) type.id);
    }

    private void writeSpatiotemporalGeometry(Kryo kryo, Output out, SpatiotemporalGeometry spatiotemporalGeometry) {
        if(spatiotemporalGeometry instanceof SpatiotemporalPoint) {
            SpatiotemporalPoint point = (SpatiotemporalPoint) spatiotemporalGeometry;
            Geometry spatialPoint = point.getSpatialGeometry();
            ZoneId zoneId = point.getZoneId();
            LocalDateTime startTime = point.getStartTime();
            LocalDateTime endTime = point.getEndTime();
            writeGeometry(kryo, out, spatialPoint);
            writeTime(out, startTime, zoneId);
            writeTime(out, endTime, zoneId);
        } else {
            throw new UnsupportedOperationException("Cannot serialize object of spatiotemporalGeometry: " + spatiotemporalGeometry.getClass().getName());
        }
    }

    private void writeGeometry(Kryo kryo, Output out, Geometry geometry) {
        out.writeBoolean(geometry != null);
        if(geometry != null) {
            byte[] data = GeometrySerde.serialize(geometry);
            out.write(data, 0, data.length);
            writeUserData(kryo, out, geometry);
        }
    }

    private void writeUserData(Kryo kryo, Output out, Geometry geometry) {
        out.writeBoolean(geometry.getUserData() != null);
        if(geometry.getUserData() != null) {
            kryo.writeClass(out, geometry.getUserData().getClass());
            kryo.writeObject(out, geometry.getUserData());
        }
    }

    private void writeTime(Output out, LocalDateTime localDateTime, ZoneId zoneId) {
        out.writeBoolean(localDateTime != null && zoneId != null);
        if(localDateTime != null && zoneId != null) {
            long time = TimeUtils.getTimestampFromDateTime(localDateTime, zoneId);
            out.writeLong(time);
            out.writeString(zoneId.toString());
        }
    }

    private void writeTemporalUnit(Output out, TemporalUnit temporalUnit) {
        out.writeBoolean(temporalUnit != null);
        if(temporalUnit != null) {
            if(temporalUnit instanceof ChronoUnit) {
                out.writeString(temporalUnit.toString());
            } else {
                throw new UnsupportedOperationException("Cannot serialize object of TemporalUnit: " + temporalUnit.getClass().getName());
            }
        }
    }

    private void writeEnvelope(Output out, Envelope spatialEnvelope) {
        out.writeBoolean(spatialEnvelope != null);
        if(spatialEnvelope != null) {
            out.writeDouble(spatialEnvelope.getMinX());
            out.writeDouble(spatialEnvelope.getMaxX());
            out.writeDouble(spatialEnvelope.getMinY());
            out.writeDouble(spatialEnvelope.getMaxY());
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        byte typeId = input.readByte();
        SpatiotemporalGeometryType spatiotemporalGeometryType = SpatiotemporalGeometryType.fromId(typeId);
        switch (spatiotemporalGeometryType) {
            case SPATIOTEMPORAL_POINT: {
                return readSpatiotemporalPoint(kryo, input);
            }
            case SPATIOTEMPORAL_CIRCLE: {
                SpatiotemporalGeometry centerGeometry = readSpatiotemporalGeometry(kryo, input);
                double spatialRadius = input.readDouble();
                long temporalRadius = input.readLong();
                TemporalUnit temporalUnit = readTemporalUnit(input);

                return new SpatiotemporalCircle(centerGeometry, spatialRadius, temporalRadius, temporalUnit);
            }
            case SPATIOTEMPORAL_ENVELOPE: {
                Envelope spatialEnvelope = readEnvelope(input);
                LocalDateTime startTime = readTime(input);
                LocalDateTime endTime = readTime(input);
                int id = input.readInt();

                SpatiotemporalEnvelope envelope = new SpatiotemporalEnvelope(spatialEnvelope, startTime, endTime);
                envelope.id = id;
                return envelope;
            }
            default: {
                throw new UnsupportedOperationException("Cannot deserialize object of type: " + spatiotemporalGeometryType);
            }
        }
    }

    private SpatiotemporalGeometry readSpatiotemporalGeometry(Kryo kryo, Input input) {
        SpatiotemporalGeometryType spatiotemporalGeometryType = SpatiotemporalGeometryType.fromId(input.readByte());
        switch (spatiotemporalGeometryType) {
            case SPATIOTEMPORAL_POINT: {
                return readSpatiotemporalPoint(kryo, input);
            }
            default: {
                throw new UnsupportedOperationException("Cannot deserialize object of type: " + spatiotemporalGeometryType);
            }
        }
    }

    private SpatiotemporalPoint readSpatiotemporalPoint(Kryo kryo, Input input) {
        Point spatialPoint = (Point) readGeometry(kryo, input);
        LocalDateTime startTime = readTime(input);
        LocalDateTime endTime = readTime(input);

        return new SpatiotemporalPoint(spatialPoint, startTime, endTime);
    }

    private Geometry readGeometry(Kryo kryo, Input input) {
        if(input.readBoolean()) {
            Geometry geometry = GeometrySerde.deserialize(input, geometryFactory);
            geometry.setUserData(readUserData(kryo, input));
            return geometry;
        }
        return null;
    }

    private Object readUserData(Kryo kryo, Input input) {
        if(input.readBoolean()) {
            Registration clazz = kryo.readClass(input);
            return kryo.readObject(input, clazz.getType());
        }
        return null;
    }

    private LocalDateTime readTime(Input input) {
        if(input.readBoolean()) {
            long time = input.readLong();
            ZoneId zoneId = ZoneId.of(input.readString());
            return TimeUtils.getDateTimeFromTimestamp(time, zoneId);
        } else {
            return null;
        }
    }

    private TemporalUnit readTemporalUnit(Input input) {
        if(input.readBoolean()) {
            String unitName = input.readString();
            for(ChronoUnit chronoUnit: ChronoUnit.values()) {
                if(chronoUnit.toString().equals(unitName)) {
                    return chronoUnit;
                }
            }
            return null;
        } else {
            return null;
        }
    }

    private Envelope readEnvelope(Input input) {
        if(input.readBoolean()) {
            double minX = input.readDouble();
            double maxX = input.readDouble();
            double minY = input.readDouble();
            double maxY = input.readDouble();

            return new Envelope(minX, maxX, minY, maxY);
        } else {
            return null;
        }
    }
}
