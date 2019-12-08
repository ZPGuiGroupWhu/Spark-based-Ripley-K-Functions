package org.whu.geoai_stval.spark_K_functions.space_time_K.analysis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RipleysKResult {
    private List Kest;
    private List Kmin;
    private List Kmax;
    private double maxSpatialDistance;
    private long maxTemporalDistance;
    private TemporalUnit temporalUnit;
    private long estimationTime;
    private long simulationTime;
    private int numPartitions;

    public RipleysKResult() {
        Kest = new ArrayList();
        Kmin = new ArrayList();
        Kmax = new ArrayList();
        temporalUnit = ChronoUnit.YEARS;
        estimationTime = 0;
        simulationTime = 0;
        numPartitions = 0;
    }

    public RipleysKResult(List Kest, List Kmin, List Kmax, double maxSpatialDistance, long maxTemporalDistance,
                          TemporalUnit temporalUnit, long estimationTime, long simulationTime, int numPartitions) {
        this.Kest = Kest;
        this.Kmin = Kmin;
        this.Kmax = Kmax;
        this.maxSpatialDistance = maxSpatialDistance;
        this.maxTemporalDistance = maxTemporalDistance;
        this.temporalUnit = temporalUnit;
        this.estimationTime = estimationTime;
        this.simulationTime = simulationTime;
        this.numPartitions = numPartitions;
    }

    public List getKest() {
        return Kest;
    }

    public void setKest(List Kest) {
        this.Kest = Kest;
    }

    public List getKmin() {
        return Kmin;
    }

    public void setKmin(List Kmin) {
        this.Kmin = Kmin;
    }

    public List getKmax() {
        return Kmax;
    }

    public void setKmax(List Kmax) {
        this.Kmax = Kmax;
    }

    public double getMaxSpatialDistance() {
        return maxSpatialDistance;
    }

    public void setMaxSpatialDistance(double maxSpatialDistance) {
        this.maxSpatialDistance = maxSpatialDistance;
    }

    public long getMaxTemporalDistance() {
        return maxTemporalDistance;
    }

    public void setMaxTemporalDistance(long maxTemporalDistance) {
        this.maxTemporalDistance = maxTemporalDistance;
    }

    public TemporalUnit getTemporalUnit() {
        return temporalUnit;
    }

    public void setTemporalUnit(TemporalUnit temporalUnit) {
        this.temporalUnit = temporalUnit;
    }

    public long getEstimationTime() {
        return estimationTime;
    }

    public void setEstimationTime(long estimationTime) {
        this.estimationTime = estimationTime;
    }

    public long getSimulationTime() {
        return simulationTime;
    }

    public void setSimulationTime(long simulationTime) {
        this.simulationTime = simulationTime;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String toJsonString() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(this);
    }

    public static void main(String[] args) throws IOException {
//        List Kest = new ArrayList(3);
//        for(int i=0; i<3; i++) {
//            List<Double> K = new ArrayList<>(3);
//            K.add(1.5);
//            K.add(2.6);
//            K.add(3.7);
//            Kest.add(K);
//        }
//
//        List<List> Kmin = new ArrayList<>(3);
//        for(int i=0; i<3; i++) {
//            List<Double> K = new ArrayList<>(3);
//            K.add(0.3);
//            K.add(0.7);
//            K.add(1.2);
//            Kmin.add(K);
//        }
//
//        List<List> Kmax = new ArrayList<>(3);
//        for(int i=0; i<3; i++) {
//            List<Double> K = new ArrayList<>(3);
//            K.add(0.9);
//            K.add(1.5);
//            K.add(2.1);
//            Kmax.add(K);
//        }
//
//        RipleysKResult result = new RipleysKResult(Kest, Kmin, Kmax, 100.0, 30, ChronoUnit.DAYS, 0, 0, 0);
//        ObjectMapper mapper = new ObjectMapper();
//        String str = mapper.writeValueAsString(result);
//        System.out.println(str);
        String directoryPath = args[0];
        String names = args[1];
        String[] fileNames = names.split(",");

        double[][] kest = new double[20][20];
        double[][] kmax = new double[20][20];
        double[][] kmin = new double[20][20];
        for(double[] kmaxRow: kmax) {
            Arrays.fill(kmaxRow, -Double.MAX_VALUE);
        }
        for(double[] kminRow: kmin) {
            Arrays.fill(kminRow, Double.MAX_VALUE);
        }
        for(int i=0; i<fileNames.length; i++) {
            String filePath = directoryPath + "/" + fileNames[i];
            byte[] resultJson = Files.readAllBytes(Paths.get(filePath));
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode kestNodes = objectMapper.readTree(resultJson).path("kest");
            JsonNode kmaxNodes = objectMapper.readTree(resultJson).path("kmax");
            JsonNode kminNodes = objectMapper.readTree(resultJson).path("kmin");

            for(int m=0; m<20; m++) {
                for(int n=0; n<20; n++) {
                    kest[m][n] = kestNodes.get(m+1).get(n+1).asDouble();

                    double kmaxInFile = kmaxNodes.get(m+1).get(n+1).asDouble();
                    if(kmax[m][n] < kmaxInFile) {
                        kmax[m][n] = kmaxInFile;
                    }

                    double kminInFile = kminNodes.get(m+1).get(n+1).asDouble();
                    if(kminInFile < kmin[m][n]) {
                        kmin[m][n] = kminInFile;
                    }
                }
            }
        }

        output4Origin(kest, kmax, kmin);
    }

    private static void output4Origin(double[][] kest, double[][] kmax, double[][] kmin) throws IOException {
        System.out.println("kest:");
        for(int m=0; m<20; m++) {
            for(int n=0; n<20; n++) {
                System.out.print(kest[m][n]);
                if(n != 19) {
                    System.out.print(",");
                }
            }
            System.out.println();
        }

        System.out.println("kmax:");
        for(int m=0; m<20; m++) {
            for(int n=0; n<20; n++) {
                System.out.print(kmax[m][n]);
                if(n != 19) {
                    System.out.print(",");
                }
            }
            System.out.println();
        }

        System.out.println("kmin:");
        for(int m=0; m<20; m++) {
            for(int n=0; n<20; n++) {
                System.out.print(kmin[m][n]);
                if(n != 19) {
                    System.out.print(",");
                }
            }
            System.out.println();
        }
    }

    private static void output4Viz(double[][] kest, double[][] kmax, double[][] kmin) throws IOException {

        System.out.print("\"kest\": [");
        for(int m=0; m<20; m++) {
            System.out.print("[");
            for(int n=0; n<20; n++) {
                System.out.print(kest[m][n]);
                if(n != 19) {
                    System.out.print(", ");
                }
            }
            System.out.print("]");
            if(m != 19) {
                System.out.print(", ");
            }
        }
        System.out.println("], ");

        System.out.print("\"kmax\": [");
        for(int m=0; m<20; m++) {
            System.out.print("[");
            for(int n=0; n<20; n++) {
                System.out.print(kmax[m][n]);
                if(n != 19) {
                    System.out.print(", ");
                }
            }
            System.out.print("]");
            if(m != 19) {
                System.out.print(", ");
            }
        }
        System.out.println("], ");

        System.out.print("\"kmin\": [");
        for(int m=0; m<20; m++) {
            System.out.print("[");
            for(int n=0; n<20; n++) {
                System.out.print(kmin[m][n]);
                if(n != 19) {
                    System.out.print(", ");
                }
            }
            System.out.print("]");
            if(m != 19) {
                System.out.print(", ");
            }
        }
        System.out.println("]");
    }
}
