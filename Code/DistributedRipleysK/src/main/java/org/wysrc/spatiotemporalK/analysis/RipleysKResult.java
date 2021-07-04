package org.wysrc.spatiotemporalK.analysis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;

public class RipleysKResult {
    private List Kest;
    private List Kmin;
    private List Kmax;
    private double maxSpatialDistance;
    private long maxTemporalDistance;
    private TemporalUnit temporalUnit;

    public RipleysKResult() {
        Kest = new ArrayList();
        Kmin = new ArrayList();
        Kmax = new ArrayList();
        temporalUnit = ChronoUnit.YEARS;
    }

    public RipleysKResult(List Kest, List Kmin, List Kmax, double maxSpatialDistance, long maxTemporalDistance, TemporalUnit temporalUnit) {
        this.Kest = Kest;
        this.Kmin = Kmin;
        this.Kmax = Kmax;
        this.maxSpatialDistance = maxSpatialDistance;
        this.maxTemporalDistance = maxTemporalDistance;
        this.temporalUnit = temporalUnit;
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

    public String toJsonString() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(this);
    }

    public static void main(String[] args) throws IOException {
        List Kest = new ArrayList(3);
        for(int i=0; i<3; i++) {
            List<Double> K = new ArrayList<>(3);
            K.add(1.5);
            K.add(2.6);
            K.add(3.7);
            Kest.add(K);
        }

        List<List> Kmin = new ArrayList<>(3);
        for(int i=0; i<3; i++) {
            List<Double> K = new ArrayList<>(3);
            K.add(0.3);
            K.add(0.7);
            K.add(1.2);
            Kmin.add(K);
        }

        List<List> Kmax = new ArrayList<>(3);
        for(int i=0; i<3; i++) {
            List<Double> K = new ArrayList<>(3);
            K.add(0.9);
            K.add(1.5);
            K.add(2.1);
            Kmax.add(K);
        }

        RipleysKResult result = new RipleysKResult(Kest, Kmin, Kmax, 100.0, 30, ChronoUnit.DAYS);
        ObjectMapper mapper = new ObjectMapper();
        String str = mapper.writeValueAsString(result);
        System.out.println(str);
    }
}
