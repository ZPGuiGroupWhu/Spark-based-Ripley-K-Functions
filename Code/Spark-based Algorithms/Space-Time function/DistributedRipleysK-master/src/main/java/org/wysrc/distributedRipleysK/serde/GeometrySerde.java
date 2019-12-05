package org.wysrc.distributedRipleysK.serde;

import com.esotericsoftware.kryo.io.Input;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class GeometrySerde {
    private static final int DOUBLE_LENGTH = 8;
    private static final int POINT_LENGTH = 1 + 2 * DOUBLE_LENGTH;

    private enum GeometryType {
        POINT(0);

        private final int id;

        GeometryType(int id) {
            this.id = id;
        }

        public static GeometryType fromId(int id) {
            for(GeometryType geometryType: GeometryType.values()){
                if(geometryType.id == id) {
                    return geometryType;
                }
            }
            return null;
        }

        public int getId() {
            return id;
        }
    }

    public static byte[] serialize(Geometry geometry) {
        if(geometry instanceof Point) {
            return serialize((Point) geometry);
        } else {
            throw new UnsupportedOperationException("Geometry type is not supported: " + geometry.getClass().getSimpleName());
        }
    }

    public static Geometry deserialize(Input input, GeometryFactory geometryFactory) {
        GeometryType geometryType = GeometryType.fromId(input.readByte());
        switch (geometryType) {
            case POINT: {
                double x = toByteBuffer(input, DOUBLE_LENGTH).getDouble();
                double y = toByteBuffer(input, DOUBLE_LENGTH).getDouble();
                Point point = geometryFactory.createPoint(new Coordinate(x, y));
                return point;
            }
            default: {
                throw new UnsupportedOperationException("Geometry type is not supported: " + geometryType);
            }
        }
    }

    private static byte[] serialize(Point point) {
        ByteBuffer byteBuffer = createBuffer(POINT_LENGTH);
        byteBuffer.put((byte) GeometryType.POINT.getId());
        byteBuffer.putDouble(point.getX());
        byteBuffer.putDouble(point.getY());
        return byteBuffer.array();
    }

    private static ByteBuffer createBuffer(int size) {
        return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static ByteBuffer toByteBuffer(Input input, int numBytes) {
        byte[] bytes = new byte[numBytes];
        input.read(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }
}
