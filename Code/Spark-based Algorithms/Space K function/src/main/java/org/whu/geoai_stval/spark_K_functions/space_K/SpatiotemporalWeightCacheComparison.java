package org.whu.geoai_stval.spark_K_functions.space_K;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.*;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.whu.geoai_stval.spark_K_functions.space_K.analysis.EdgeCorrection;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalCircle;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalEnvelope;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalPoint;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalPolygon;
import org.whu.geoai_stval.spark_K_functions.space_K.utils.TimeUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SpatiotemporalWeightCacheComparison {
    private static final int dataSize = 50000;
    private static final int simulationCount = 20;
    private static final String pointPath = System.getProperty("user.dir")+"/src/test/resources/" + "hubei.csv";
    private static final String polygonPath = System.getProperty("user.dir")+"/src/test/resources/" + "hubei-973.json";
    private static double spatialRadius = 20000;
    private static long temporalRadius = 300;
    private static TemporalUnit temporalUnit = ChronoUnit.DAYS;
    private static HashMap<Point, HashMap<Double, Double>> spatialWeightCache;
    private static HashMap<LocalDateTime, HashMap<Long, Double>> temporalWeightCache;
    private static double spatialLocationTolerance = 1;
    private static double spatialDistanceTolerance = 100;
    private static GeometryFactory geometryFactory = new GeometryFactory();
    private static int spatialStep = 20;
    private static int temporalStep = 20;

    public static void main(String[] args) throws IOException, FactoryException, TransformException {
        compareTime(dataSize, simulationCount, pointPath, polygonPath, spatialRadius, temporalRadius, temporalUnit,
                spatialStep, temporalStep);
    }

    private static void compareTime(int dataSize, int simulationCount, String pointPath, String polygonPath,
                                    double spatialRadius, long temporalRadius, TemporalUnit temporalUnit,
                                    int spatialStep, int temporalStep) throws IOException, FactoryException, TransformException {
        System.out.println("\n==== " + new Date() + ": Compare K function time with Weight Cache ====");
        long before, after, noCacheEstimationTime = 0, useCacheEstimationTime = 0,
                noCacheSimulationTime = 0, useCacheSimulationTime = 0;
        double[][] KValues;
        double[][] kmax = new double[spatialStep][temporalStep];
        double[][] kmin = new double[spatialStep][temporalStep];
        for(double[] kmaxRow: kmax) {
            Arrays.fill(kmaxRow, -Double.MAX_VALUE);
        }
        for(double[] kminRow: kmin) {
            Arrays.fill(kminRow, Double.MAX_VALUE);
        }
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
        SpatiotemporalPolygon spatiotemporalBoundary = readBoundary(false, envelope, polygonPath);

        // estimation WITHOUT Cache
//        before = System.currentTimeMillis();
//        KValues = calculateKFunction(points, circles, spatiotemporalBoundary, spatialRadius, temporalRadius, spatialStep, temporalStep,
//                false, true);
//        System.out.println("[Estimation] K Value with no cache: " + Arrays.deepToString(KValues));
//        after = System.currentTimeMillis();
//        noCacheEstimationTime += (after - before);

        // estimation WITH Cache
        before = System.currentTimeMillis();
        KValues = calculateKFunction(points, circles, spatiotemporalBoundary, spatialRadius, temporalRadius, spatialStep, temporalStep,
                true, true);
        System.out.println("[Estimation] K Value using cache: "+ Arrays.deepToString(KValues));
        after = System.currentTimeMillis();
        useCacheEstimationTime += (after - before);

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

            // simulation WITHOUT cache
//            before = System.currentTimeMillis();
//            KValues = calculateKFunction(points, circles, spatiotemporalBoundary, spatialRadius, temporalRadius, spatialStep, temporalStep,
//                    false, false);
//            System.out.println("[Simulation] K Value with no cache: " + Arrays.deepToString(KValues));
//            after = System.currentTimeMillis();
//            noCacheSimulationTime += (after - before);

            // simulation WITH cache
            before = System.currentTimeMillis();
            KValues = calculateKFunction(points, circles, spatiotemporalBoundary, spatialRadius, temporalRadius, spatialStep, temporalStep,
                    true, false);
            for(int m=0; m<spatialStep; m++) {
                for(int n=0; n<temporalStep; n++) {
                    double kInValue = KValues[m+1][n+1];
                    if(kmax[m][n] < kInValue) {
                        kmax[m][n] = kInValue;
                    }

                    if(kInValue < kmin[m][n]) {
                        kmin[m][n] = kInValue;
                    }
                }
            }
//            System.out.println("[Simulation] K Value using cache: " + Arrays.deepToString(KValues));
            after = System.currentTimeMillis();
            useCacheSimulationTime += (after - before);
        }

        System.out.println("[Simulation] K max Value using cache: " + Arrays.deepToString(kmax));
        System.out.println("[Simulation] K min Value using cache: " + Arrays.deepToString(kmin));

        if(simulationCount > 0) {
            noCacheSimulationTime /= simulationCount;
            useCacheSimulationTime /= simulationCount;
        }

        System.out.println("Time cost:");
        System.out.println("[Estimation] No cache time: " + noCacheEstimationTime + " ms");
        System.out.println("[Estimation] Using cache time: " + useCacheEstimationTime + " ms");

        System.out.println("[Simulation] No cache time: " + noCacheSimulationTime + " ms");
        System.out.println("[Simulation] Using cache time: " + useCacheSimulationTime + " ms");
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

    private static double[][] calculateKFunction(List<SpatiotemporalPoint> points, List<SpatiotemporalCircle> circles,
                                                 SpatiotemporalPolygon spatiotemporalBoundary, double maxSpatialDistance,
                                                 long maxTemporalDistance, int spatialStep, int temporalStep,
                                                 boolean useCache, boolean isEstimation) {
        double[][] KValues = new double[spatialStep + 1][temporalStep + 1];
        double deltaSpatialWeight = 0.0, deltaTemporalWeight = 0.0;
        long spatialCacheHit = 0, temporalCacheHit = 0;
        long pairCount = 0;
        if(useCache) {
            if(isEstimation) {
                if(spatialWeightCache == null) {
                    spatialWeightCache = new HashMap<>(4096);
                }
                if(temporalWeightCache == null) {
                    temporalWeightCache = new HashMap<>(1024);
                }
                spatialWeightCache.clear();
                temporalWeightCache.clear();
                for(SpatiotemporalCircle circle: circles) {
                    for(SpatiotemporalPoint point: points) {
                        if(circle.intersects(point)) {
                            pairCount++;
                            Point centerPoint = (Point) circle.getSpatialGeometry();
                            Point neighborPoint = (Point) point.getSpatialGeometry();
                            LocalDateTime centerTime = circle.getCenterSpatiotemporalGeometry().getStartTime();
                            LocalDateTime neighborTime = point.getStartTime();
                            double pairSpatialDistance = calSpatialDistance(centerPoint, neighborPoint);
                            long pairTemporalDistance = TimeUtils.calTemporalDistance(centerTime, neighborTime, temporalUnit);
                            Point roundCenterPoint = geometryFactory.createPoint(new Coordinate(Math.round(centerPoint.getX()/spatialLocationTolerance)*spatialLocationTolerance,
                                    Math.round(centerPoint.getY()/spatialLocationTolerance)*spatialLocationTolerance));
                            double roundSpatialDistance = Math.round(pairSpatialDistance/spatialDistanceTolerance)*spatialDistanceTolerance;
                            LocalDateTime roundCenterTime = LocalDateTime.parse(getSimplifiedTime(centerTime, temporalUnit));

                            // put spatialWeight
                            if(!spatialWeightCache.containsKey(roundCenterPoint)) {
                                spatialWeightCache.put(roundCenterPoint, new HashMap<>(32));
                            }
                            double spatialWeight;
                            if(!spatialWeightCache.get(roundCenterPoint).containsKey(roundSpatialDistance)) {
                                spatialWeight = EdgeCorrection.spatialIsotropicCorrection(centerPoint, neighborPoint,
                                        (Polygon) spatiotemporalBoundary.getSpatialGeometry());
                                spatialWeightCache.get(roundCenterPoint).put(roundSpatialDistance, spatialWeight);
                            } else {
                                spatialWeight = spatialWeightCache.get(roundCenterPoint).get(roundSpatialDistance);
                                spatialCacheHit++;
//                                double tempWeight = EdgeCorrection.spatialIsotropicCorrection(centerPoint, neighborPoint,
//                                        (Polygon) spatiotemporalBoundary.getSpatialGeometry());
//                                deltaSpatialWeight += (spatialWeight-tempWeight)*(spatialWeight-tempWeight);
                            }

//                            double temporalWeight = EdgeCorrection.temporalIsotropicCorrection(centerTime, neighborTime,
//                                        spatiotemporalBoundary.getStartTime(), spatiotemporalBoundary.getEndTime(), temporalUnit);

                            // put temporalWeight
                            if(!temporalWeightCache.containsKey(centerTime)) {
                                temporalWeightCache.put(centerTime, new HashMap<>(32));
                            }
                            double temporalWeight;
                            if(!temporalWeightCache.get(centerTime).containsKey(pairTemporalDistance)) {
                                temporalWeight = EdgeCorrection.temporalIsotropicCorrection(centerTime, neighborTime,
                                        spatiotemporalBoundary.getStartTime(), spatiotemporalBoundary.getEndTime(), temporalUnit);
                                temporalWeightCache.get(centerTime).put(pairTemporalDistance, temporalWeight);
                            } else {
                                temporalWeight = temporalWeightCache.get(centerTime).get(pairTemporalDistance);
                                temporalCacheHit++;
//                                double tempWeight = EdgeCorrection.temporalIsotropicCorrection(centerTime, neighborTime,
//                                        spatiotemporalBoundary.getStartTime(), spatiotemporalBoundary.getEndTime(), temporalUnit);
//                                deltaTemporalWeight += (temporalWeight-tempWeight)*(temporalWeight-tempWeight);
                            }

                            for(int i=spatialStep; i>0; i--) {
                                double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
                                if(pairSpatialDistance > currentSpatialDistance) {
                                    break;
                                }
                                for(int j=temporalStep; j>0; j--) {
                                    long currentTemporalDistance = (maxTemporalDistance / temporalStep) * j;
                                    if(pairTemporalDistance > currentTemporalDistance) {
                                        break;
                                    }
                                    KValues[i][j] += spatialWeight * temporalWeight;
                                }
                            }
                        }
                    }
                }

//                double RMSESpatial = Math.sqrt(deltaSpatialWeight/(circles.size()*points.size()));
//                double RMSETemporal = Math.sqrt(deltaTemporalWeight/(circles.size()*points.size()));
//                System.out.println("RMSE of spatial weight: " + RMSESpatial);
//                System.out.println("RMSE of temporal weight: " + RMSETemporal);
            } else {
                for(SpatiotemporalCircle circle: circles) {
                    for(SpatiotemporalPoint point: points) {
                        if(circle.intersects(point)) {
                            pairCount++;
                            Point centerPoint = (Point) circle.getSpatialGeometry();
                            Point neighborPoint = (Point) point.getSpatialGeometry();
                            LocalDateTime centerTime = circle.getCenterSpatiotemporalGeometry().getStartTime();
                            LocalDateTime neighborTime = point.getStartTime();
                            double pairSpatialDistance = calSpatialDistance(centerPoint, neighborPoint);
                            long pairTemporalDistance = TimeUtils.calTemporalDistance(centerTime, neighborTime, temporalUnit);
                            Point roundCenterPoint = geometryFactory.createPoint(new Coordinate(Math.round(centerPoint.getX()/spatialLocationTolerance)*spatialLocationTolerance,
                                    Math.round(centerPoint.getY()/spatialLocationTolerance)*spatialLocationTolerance));
                            double roundSpatialDistance = Math.round(pairSpatialDistance/spatialDistanceTolerance)*spatialDistanceTolerance;
                            LocalDateTime roundCenterTime = LocalDateTime.parse(getSimplifiedTime(centerTime, temporalUnit));

                            // get spatialWeight
                            double spatialWeight;
                            if(!spatialWeightCache.get(roundCenterPoint).containsKey(roundSpatialDistance)) {
                                spatialWeight = EdgeCorrection.spatialIsotropicCorrection(centerPoint, neighborPoint,
                                        (Polygon) spatiotemporalBoundary.getSpatialGeometry());
                            } else {
                                spatialWeight = spatialWeightCache.get(roundCenterPoint).get(roundSpatialDistance);
                                spatialCacheHit++;
                            }

//                            double temporalWeight = EdgeCorrection.temporalIsotropicCorrection(centerTime, neighborTime,
//                                    spatiotemporalBoundary.getStartTime(), spatiotemporalBoundary.getEndTime(), temporalUnit);

                            // get temporalWeight
                            double temporalWeight;
                            if(!temporalWeightCache.get(centerTime).containsKey(pairTemporalDistance)) {
                                temporalWeight = EdgeCorrection.temporalIsotropicCorrection(centerTime, neighborTime,
                                        spatiotemporalBoundary.getStartTime(), spatiotemporalBoundary.getEndTime(), temporalUnit);
                            } else {
                                temporalWeight = temporalWeightCache.get(centerTime).get(pairTemporalDistance);
                                temporalCacheHit++;
                            }

                            for(int i=spatialStep; i>0; i--) {
                                double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
                                if(pairSpatialDistance > currentSpatialDistance) {
                                    break;
                                }
                                for(int j=temporalStep; j>0; j--) {
                                    long currentTemporalDistance = (maxTemporalDistance / temporalStep) * j;
                                    if(pairTemporalDistance > currentTemporalDistance) {
                                        break;
                                    }
                                    KValues[i][j] += spatialWeight * temporalWeight;
                                }
                            }
                        }
                    }
                }
            }

            System.out.println("Spatial cache hit count: " + spatialCacheHit);
            System.out.println("Spatial cache hit proportion: " + 100.0 * spatialCacheHit/pairCount + " %");
            System.out.println("Temporal cache hit count: " + temporalCacheHit);
            System.out.println("Temporal cache hit proportion: " + 100.0 * temporalCacheHit/pairCount + " %");
        } else {
            for(SpatiotemporalCircle circle: circles) {
                for(SpatiotemporalPoint point: points) {
                    if(circle.intersects(point)) {
                        double spatiotemporalWeight = EdgeCorrection.isotropicCorrection((SpatiotemporalPoint) circle.getCenterSpatiotemporalGeometry(),
                                point, spatiotemporalBoundary, temporalUnit);
                        Point centerPoint = (Point) circle.getSpatialGeometry();
                        Point neighborPoint = (Point) point.getSpatialGeometry();
                        LocalDateTime centerTime = circle.getCenterSpatiotemporalGeometry().getStartTime();
                        LocalDateTime neighborTime = point.getStartTime();
                        double pairSpatialDistance = calSpatialDistance(centerPoint, neighborPoint);
                        long pairTemporalDistance = TimeUtils.calTemporalDistance(centerTime, neighborTime, temporalUnit);

                        for(int i=spatialStep; i>0; i--) {
                            double currentSpatialDistance = (maxSpatialDistance / spatialStep) * i;
                            if(pairSpatialDistance > currentSpatialDistance) {
                                break;
                            }
                            for(int j=temporalStep; j>0; j--) {
                                long currentTemporalDistance = (maxTemporalDistance / temporalStep) * j;
                                if(pairTemporalDistance > currentTemporalDistance) {
                                    break;
                                }
                                KValues[i][j] += spatiotemporalWeight;
                            }
                        }
                    }
                }
            }
        }

        double intensity = dataSize / spatiotemporalBoundary.getVolume(temporalUnit);
        for(int i=1; i<spatialStep + 1; i++) {
            for(int j=1; j<temporalStep+1; j++) {
                KValues[i][j] = Math.sqrt((KValues[i][j]-dataSize)/(dataSize*intensity*Math.PI*2*(maxTemporalDistance/temporalStep)*j))
                        - (maxSpatialDistance/spatialStep)*i;
            }
        }

        return KValues;
    }

    private static void CRSTransform(List<SpatiotemporalPoint> points, String sourceEPSGCode, String targetEPSGCode) throws FactoryException, TransformException {
        CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEPSGCode);
        CoordinateReferenceSystem targetCRS = CRS.decode(targetEPSGCode);
        final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
        for(SpatiotemporalPoint point: points) {
            point.setSpatialGeometry(JTS.transform(point.getSpatialGeometry(), transform));
        }
    }

    private static double calSpatialDistance(Point point1, Point point2) {
        return Math.sqrt((point1.getX() - point2.getX())*(point1.getX() - point2.getX()) +
                (point1.getY() - point2.getY())*(point1.getY() - point2.getY()));
    }

    private static String getSimplifiedTime(LocalDateTime dateTime, TemporalUnit temporalUnit) {
        if(temporalUnit instanceof ChronoUnit) {
            ChronoUnit chronoUnit = (ChronoUnit) temporalUnit;
            switch (chronoUnit) {
                case YEARS: {
                    return LocalDateTime.of(dateTime.getYear(), 1, 1, 0, 0).toString();
                }
                case MONTHS: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), 1, 0, 0).toString();
                }
                case DAYS: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            0, 0, 0).toString();
                }
                case HOURS: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            dateTime.getHour(), 0, 0).toString();
                }
                case MINUTES: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            dateTime.getHour(), dateTime.getMinute(), 0).toString();
                }
                case SECONDS: {
                    return LocalDateTime.of(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                            dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()).toString();
                }
            }
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + temporalUnit);
        }
        return dateTime.toString();
    }
}

