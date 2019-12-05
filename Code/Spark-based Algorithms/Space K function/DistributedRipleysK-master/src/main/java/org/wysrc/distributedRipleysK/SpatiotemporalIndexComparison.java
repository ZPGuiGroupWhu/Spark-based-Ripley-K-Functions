package org.wysrc.distributedRipleysK;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.wysrc.distributedRipleysK.analysis.EdgeCorrection;
import org.wysrc.distributedRipleysK.geom.*;
import org.wysrc.distributedRipleysK.index.SpatiotemporalIndex;
import org.wysrc.distributedRipleysK.index.strtree.STRTree;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SpatiotemporalIndexComparison {
    private static final int dataSize = 50000;
    private static final int simulationCount = 3;
    private static final String pointPath = System.getProperty("user.dir")+"/src/test/resources/" + "hubei.csv";
    private static final String polygonPath = System.getProperty("user.dir")+"/src/test/resources/" + "hubei-973.json";
    private static double spatialRadius = 120000;
    private static long temporalRadius = 5;
    private static TemporalUnit temporalUnit = ChronoUnit.MONTHS;

    public static void main(String[] args) throws IOException, FactoryException, TransformException {
        compareTime(dataSize, simulationCount, STRTree.class, pointPath, polygonPath, spatialRadius, temporalRadius, temporalUnit);
    }

    private static void compareTime(int dataSize, int simulationCount, Class aClass, String pointPath, String polygonPath, double spatialRadius, long temporalRadius, TemporalUnit temporalUnit) throws IOException, FactoryException, TransformException {
        System.out.println("\n==== " + new Date() + ": Compare K function time with " + aClass.toString() + " ====");
        long before, middle, after, noIndexEstimationTime = 0, buildIndexEstimationTime = 0, queryIndexEstimationTime = 0,
                noIndexSimulationTime = 0, buildIndexSimulationTime = 0, queryIndexSimulationTime = 0;
        double KValue;
        List<SpatiotemporalPoint> points = readPointData(pointPath, dataSize);
        CRSTransform(points, "epsg:4326", "epsg:3857");
        SpatiotemporalEnvelope envelope = new SpatiotemporalEnvelope();
        List<SpatiotemporalCircle> circles = new ArrayList<>();
        List<LocalDateTime> timeLabels = new ArrayList<>();
        for(SpatiotemporalPoint point: points) {
            envelope.expandToInclude(point);
            circles.add(new SpatiotemporalCircle(point, spatialRadius, temporalRadius, temporalUnit));
            timeLabels.add(point.getStartTime());
        }
        SpatiotemporalPolygon spatiotemporalBoundary = readBoundary(true, envelope, polygonPath);

        // estimation WITHOUT Index
        before = System.currentTimeMillis();
        KValue = calculateKFunction(points, circles, spatiotemporalBoundary, null);
        System.out.println("[Estimation] K Value with no index: "+KValue);
        after = System.currentTimeMillis();
        noIndexEstimationTime += (after - before);

        // estimation WITH index
        before = System.currentTimeMillis();
        SpatiotemporalIndex index = buildIndex(points, aClass);
        middle = System.currentTimeMillis();
        KValue = calculateKFunction(points, circles, spatiotemporalBoundary, index);
        System.out.println("[Estimation] K Value using index: "+KValue);
        after = System.currentTimeMillis();
        buildIndexEstimationTime += (middle - before);
        queryIndexEstimationTime += (after - middle);

        // simulations
        for(int i=0; i<simulationCount; i++) {
            Collections.shuffle(timeLabels);
            for(int p=0; p<points.size(); p++) {
                points.get(p).setStartTime(timeLabels.get(p));
            }
            circles.clear();
            for(SpatiotemporalPoint point: points) {
                circles.add(new SpatiotemporalCircle(point, spatialRadius, temporalRadius, temporalUnit));
            }

            // simulation WITHOUT Index
            before = System.currentTimeMillis();
            KValue = calculateKFunction(points, circles, spatiotemporalBoundary, null);
            System.out.println("[Simulation] K Value with no index: "+KValue);
            after = System.currentTimeMillis();
            noIndexSimulationTime += (after - before);

            // simulation WITH Index
            before = System.currentTimeMillis();
            SpatiotemporalIndex indexForSimulation = buildIndex(points, aClass);
            middle = System.currentTimeMillis();
            KValue = calculateKFunction(points, circles, spatiotemporalBoundary, indexForSimulation);
            System.out.println("[Simulation] K Value using index: "+KValue);
            after = System.currentTimeMillis();
            buildIndexSimulationTime += (middle - before);
            queryIndexSimulationTime += (after - middle);
        }

        if(simulationCount > 0) {
            noIndexSimulationTime /= simulationCount;
            buildIndexSimulationTime /= simulationCount;
            queryIndexSimulationTime /= simulationCount;
        }

        System.out.println("Time cost:");
        System.out.println("[Estimation] No index time: " + noIndexEstimationTime + " ms");
        System.out.println("[Estimation] Using index time: " + (buildIndexEstimationTime+queryIndexEstimationTime) + " ms");
        System.out.println("[Estimation] Build index time: " + buildIndexEstimationTime + " ms");
        System.out.println("[Estimation] Query index time: " + queryIndexEstimationTime + " ms");

        System.out.println("[Simulation] No index time: " + noIndexSimulationTime + " ms");
        System.out.println("[Simulation] Using index time: " + (buildIndexSimulationTime+queryIndexSimulationTime) + " ms");
        System.out.println("[Simulation] Build index time: " + buildIndexSimulationTime + " ms");
        System.out.println("[Simulation] Query index time: " + queryIndexSimulationTime + " ms");
    }

    private static List<SpatiotemporalPoint> readPointData(String pointPath, int dataSize) throws FileNotFoundException {
        List<SpatiotemporalPoint> result = new ArrayList<>();
        File inputFile = new File(pointPath);
        InputStream inputStream = new FileInputStream(inputFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        result = reader.lines().limit(dataSize).map(mapToPoint).collect(Collectors.toList());
        return result;
    }

    private static SpatiotemporalPolygon readBoundary(boolean useEnvelopeBoundary, SpatiotemporalEnvelope envelope, String polygonPath) throws IOException {
        if(useEnvelopeBoundary) {
            return new SpatiotemporalPolygon(envelope);
        } else {
            byte[] boundaryJson = Files.readAllBytes(Paths.get(polygonPath));
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode coordinateNodes = objectMapper.readTree(boundaryJson).path("features").get(0).path("geometry").path("coordinates").get(0);
            double[] coordinates = new double[coordinateNodes.size()*2];
            for(int i=0; i<coordinates.length/2; i++) {
                JsonNode currentCoordinate = coordinateNodes.get(i);
                coordinates[i*2] = currentCoordinate.get(0).asDouble();
                coordinates[i*2+1] = currentCoordinate.get(1).asDouble();
            }
            return new SpatiotemporalPolygon(coordinates, envelope.startTime,
                    envelope.endTime, "epsg:4326", "epsg:3857", true);
        }
    }

    private static Function<String, SpatiotemporalPoint> mapToPoint = (line) -> {
        String[] fields = line.split(",");

        double x = Double.valueOf(fields[0]);
        double y = Double.valueOf(fields[1]);
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("MM/dd/yyyy HH:mm:ss").
                parseDefaulting(ChronoField.MONTH_OF_YEAR, 6).
                parseDefaulting(ChronoField.DAY_OF_MONTH, 30).
                parseDefaulting(ChronoField.HOUR_OF_DAY, 0).
                parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).
                parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter();
        LocalDateTime dateTime = LocalDateTime.parse(fields[2], formatter);
        return new SpatiotemporalPoint(new Coordinate(x, y), dateTime);
    };

    private static double calculateKFunction(List<SpatiotemporalPoint> points, List<SpatiotemporalCircle> circles,
                                      SpatiotemporalPolygon spatiotemporalBoundary, SpatiotemporalIndex index) {
        double KValue = 0.0;
        if(index != null) {
            for(SpatiotemporalCircle circle: circles) {
                List<SpatiotemporalPoint> candidates = index.query(circle.getEnvelopeInternal());
                for(SpatiotemporalPoint candidate: candidates) {
                    if(circle.intersects(candidate)) {
                        KValue += EdgeCorrection.isotropicCorrection((SpatiotemporalPoint) circle.getCenterSpatiotemporalGeometry(),
                                candidate, spatiotemporalBoundary, temporalUnit);
                    }
                }
            }
        } else {
            for(SpatiotemporalCircle circle: circles) {
                for(SpatiotemporalPoint point: points) {
                    if(circle.intersects(point)) {
                        KValue += EdgeCorrection.isotropicCorrection((SpatiotemporalPoint) circle.getCenterSpatiotemporalGeometry(),
                                point, spatiotemporalBoundary, temporalUnit);
                    }
                }
            }
        }
        double intensity = dataSize / spatiotemporalBoundary.getVolume(temporalUnit);
        KValue = Math.sqrt((KValue-dataSize)/(dataSize*intensity*Math.PI*2*temporalRadius))-spatialRadius;
        return KValue;
    }

    private static SpatiotemporalIndex buildIndex(List<SpatiotemporalPoint> points, Class indexClass) {
        SpatiotemporalIndex index;
        if(indexClass == STRTree.class) {
            index = new STRTree();
        } else {
            throw new UnsupportedOperationException("The index type is not supported: " + indexClass.getName());
        }
        for(SpatiotemporalPoint point: points) {
            index.insert(point.getEnvelopeInternal(), point);
        }
        index.query(new SpatiotemporalEnvelope(new Envelope(0.0, 0.0, 0.0, 0.0), LocalDateTime.now()));

        return index;
    }

    private static void CRSTransform(List<SpatiotemporalPoint> points, String sourceEPSGCode, String targetEPSGCode) throws FactoryException, TransformException {
        CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEPSGCode);
        CoordinateReferenceSystem targetCRS = CRS.decode(targetEPSGCode);
        final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
        for(SpatiotemporalPoint point: points) {
            point.setSpatialGeometry(JTS.transform(point.getSpatialGeometry(), transform));
        }
    }
}
