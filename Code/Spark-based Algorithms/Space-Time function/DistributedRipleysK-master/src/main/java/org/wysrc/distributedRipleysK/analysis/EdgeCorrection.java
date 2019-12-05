package org.wysrc.distributedRipleysK.analysis;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalPoint;
import org.wysrc.distributedRipleysK.geom.SpatiotemporalPolygon;
import org.wysrc.distributedRipleysK.utils.TimeUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;

public class EdgeCorrection {
    public static final double weightMaximum = 100.0;
    public static final double weightMinimum = 0.0;
    private static boolean between(double value, double value0, double value1) {
        return (value - value0) * (value - value1) <= 0;
    }

    private static boolean under(double x, double y, double x0, double y0, double x1, double y1) {
        return (y - y0) * (x1 - x0) <= (y1 - y0) * (x - x0);
    }

    private static boolean underneath(double x, double y, double x0, double y0, double x1, double y1) {
        if(x0 < x1 || (x0 == x1 && y0 < y1)) {
            return under(x, y, x0, y0, x1, y1);
        } else {
            return under(x, y, x1, y1, x0, y0);
        }
    }

    private static boolean inside(double x, double y, double x0, double y0, double x1, double y1) {
        return between(x, x0, x1) && underneath(x, y, x0, y0, x1, y1);
    }

    public static double isotropicCorrection(SpatiotemporalPoint centerPoint, SpatiotemporalPoint neighborPoint,
                                             SpatiotemporalPolygon spatiotemporalBoundary, TemporalUnit temporalUnit) {
        Point spatialCenterPoint = (Point) centerPoint.getSpatialGeometry();
        Point spatialNeighborPoint = (Point) neighborPoint.getSpatialGeometry();
        Polygon spatialBoundary = (Polygon) spatiotemporalBoundary.getSpatialGeometry();
        LocalDateTime temporalCenterPoint = centerPoint.getStartTime();
        LocalDateTime temporalNeighborPoint = neighborPoint.getStartTime();
        LocalDateTime startTime = spatiotemporalBoundary.getStartTime();
        LocalDateTime endTime = spatiotemporalBoundary.getEndTime();

        double spatialWeight = spatialIsotropicCorrection(spatialCenterPoint, spatialNeighborPoint, spatialBoundary);
        double temporalWeight = temporalIsotropicCorrection(temporalCenterPoint, temporalNeighborPoint, startTime, endTime, temporalUnit);
        return spatialWeight * temporalWeight;
    }

    public static double spatialIsotropicCorrection(Point centerPoint, Point neighborPoint, Polygon spatialBoundary) {
        double total = 0.0;
        double centerX = centerPoint.getX();
        double centerY = centerPoint.getY();
        double neighborX = neighborPoint.getX();
        double neighborY = neighborPoint.getY();

        double radius2 = (neighborX - centerX)*(neighborX - centerX) + (neighborY - centerY)*(neighborY - centerY);
        double radius = Math.sqrt(radius2);
        Coordinate[] vertexes = spatialBoundary.getCoordinates();
        boolean clockwise = !CGAlgorithms.isCCW(vertexes);
        double det, sqrtDet, edgeX, edgeY;
        double theta[] = new double[6], delta[] = new double[7], tmid[] = new double[7];
        for(int i=0; i<vertexes.length-1; i++) {
            int ncut = 0;
            double x0 = vertexes[i].x;
            double y0 = vertexes[i].y;
            double x1 = vertexes[i+1].x;
            double y1 = vertexes[i+1].y;

            /* intersection with left edge */
            double dx0 = x0 - centerX;
            det = radius2 - dx0 * dx0;
            if(det > 0) {
                sqrtDet = Math.sqrt(det);
                edgeY = centerY + sqrtDet;
                if(edgeY < y0) {
                    theta[ncut] = Math.atan2(edgeY - centerY, dx0);
                    ncut++;
                }
                edgeY = centerY - sqrtDet;
                if(edgeY < y0) {
                    theta[ncut] = Math.atan2(edgeY - centerY, dx0);
                    ncut++;
                }
            } else if(det == 0) {
                if(centerY < y0) {
                    theta[ncut] = Math.atan2(0.0, dx0);
                    ncut++;
                }
            }

            /* intersection with right edge */
            double dx1 = x1 - centerX;
            det = radius2 - dx1 * dx1;
            if(det > 0) {
                sqrtDet = Math.sqrt(det);
                edgeY = centerY + sqrtDet;
                if(edgeY < y1) {
                    theta[ncut] = Math.atan2(edgeY - centerY, dx1);
                    ncut++;
                }
                edgeY = centerY - sqrtDet;
                if(edgeY < y1) {
                    theta[ncut] = Math.atan2(edgeY - centerY, dx1);
                    ncut++;
                }
            } else if(det == 0) {
                if(centerY < y1) {
                    theta[ncut] = Math.atan2(0.0, dx1);
                    ncut++;
                }
            }

            /* intersection with top segment */
            double x01 = x1 - x0;
            double y01 = y1 - y0;
            double dy0 = y0 - centerY;
            double a = x01 * x01 + y01 * y01;
            double b = 2 * (x01 * dx0 + y01 * dy0);
            double c = dx0 * dx0 + dy0 * dy0 - radius2;
            double t;
            det = b * b - 4 * a * c;
            if(det > 0) {
                sqrtDet = Math.sqrt(det);
                t = (sqrtDet - b) / (2 * a);
                if(t >= 0 && t <= 1) {
                    edgeX = x0 + t * x01;
                    edgeY = y0 + t * y01;
                    theta[ncut] = Math.atan2(edgeY - centerY, edgeX - centerX);
                    ncut++;
                }
                t = (-sqrtDet - b) / (2 * a);
                if(t >= 0 && t <= 1) {
                    edgeX = x0 + t * x01;
                    edgeY = y0 + t * y01;
                    theta[ncut] = Math.atan2(edgeY - centerY, edgeX - centerX);
                    ncut++;
                }
            } else if(det == 0) {
                t = -b / (2 * a);
                if(t >= 0 && t <= 1) {
                    edgeX = x0 + t * x01;
                    edgeY = y0 + t * y01;
                    theta[ncut] = Math.atan2(edgeY - centerY, edgeX - centerX);
                    ncut++;
                }
            }

            /* for safety, force all angles to be in [0, 2 * PI] */
            for(int j=0; j<ncut; j++) {
                if(theta[j] < 0) {
                    theta[j] += 2 * Math.PI;
                }
            }

            /* sort angles */
            Arrays.sort(theta, 0, ncut);

            /* compute length of circumference inside polygon */
            double testX, testY, contrib;
            if(ncut == 0) {
                /* entire circle is either in or out */
                testX = centerX + radius;
                testY = centerY;
                if(inside(testX, testY, x0, y0, x1, y1)) {
                    contrib = 2 * Math.PI;
                } else {
                    contrib = 0.0;
                }
            } else {
                /* find midpoints and length of pieces (adding theta = ) */
                delta[0] = theta[0];
                tmid[0] = theta[0] / 2;
                for(int j=1; j<ncut; j++) {
                    delta[j] = theta[j] - theta[j-1];
                    tmid[j] = (theta[j] + theta[j-1]) / 2;
                }
                delta[ncut] = 2 * Math.PI - theta[ncut - 1];
                tmid[ncut] = (2 * Math.PI + theta[ncut - 1]) / 2;

                contrib = 0.0;
                for(int j=0; j<=ncut; j++) {
                    testX = centerX + radius * Math.cos(tmid[j]);
                    testY = centerY + radius * Math.sin(tmid[j]);
                    if(inside(testX, testY, x0, y0, x1, y1)) {
                        contrib += delta[j];
                    }
                }
            }

            /* multiply by sign of trapezium */
            if((!clockwise && x0 < x1) || (clockwise && x0 > x1)) {
                contrib *= -1;
            }
            
            total += contrib;
        }

        total = 2 * Math.PI / total;
        total = Math.min(total, weightMaximum);
        total = Math.max(weightMinimum, total);
        return total;
    }

    public static double temporalIsotropicCorrection(LocalDateTime centerTime, LocalDateTime neighborTime,
                                                     LocalDateTime startTime, LocalDateTime endTime, TemporalUnit temporalUnit) {
        if(centerTime.compareTo(startTime) < 0 || centerTime.compareTo(endTime) > 0) {
            return 0.0;
        }
        long interval = TimeUtils.calTemporalDistance(centerTime, neighborTime, temporalUnit);
        if(interval == 0) {
            return 1.0;
        }
        LocalDateTime lowerTime = centerTime.minus(interval, temporalUnit);
        LocalDateTime higherTime = centerTime.plus(interval, temporalUnit);
        if(lowerTime.compareTo(startTime) >= 0 && higherTime.compareTo(endTime) <= 0) {
            return 1.0;
        } else {
            return 2.0;
        }
    }

    public static void main(String[] args) {
        GeometryFactory factory = new GeometryFactory();
        Point centerPoint = factory.createPoint(new Coordinate(3, 5));
        Point neighborPoint = factory.createPoint(new Coordinate(3, 4));
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(1, 1);
        coordinates[1] = new Coordinate(1, 5);
        coordinates[2] = new Coordinate(5, 5);
        coordinates[3] = new Coordinate(5, 1);
        coordinates[4] = new Coordinate(1, 1);
//        coordinates[0] = new Coordinate(1, 1);
//        coordinates[1] = new Coordinate(5, 1);
//        coordinates[2] = new Coordinate(5, 5);
//        coordinates[3] = new Coordinate(1, 5);
//        coordinates[4] = new Coordinate(1, 1);
        Polygon boundary = factory.createPolygon(coordinates);
//        System.out.println(boundary);
        double spatialWeight = spatialIsotropicCorrection(centerPoint, neighborPoint, boundary);
        System.out.println("spatial: "+spatialWeight);

        LocalDateTime centerTime = LocalDateTime.now();
        LocalDateTime neighborTime = centerTime.plus(20, ChronoUnit.SECONDS);
        LocalDateTime startTime = centerTime.minus(10, ChronoUnit.SECONDS);
        LocalDateTime endTime = centerTime.plus(10, ChronoUnit.SECONDS);
        double temporalWeight = temporalIsotropicCorrection(centerTime, neighborTime, startTime, endTime,  ChronoUnit.SECONDS);
        System.out.println("temporal: "+temporalWeight);
    }
}
