package org.wysrc.localK.analysis;

public class PointResult {
    Double x;
    Double y;
    Double[] k;

    public PointResult(Double x, Double y, Double[] k) {
        this.x = x;
        this.y = y;
        this.k = k;
    }

    public void setX(Double x) {
        this.x = x;
    }

    public void setY(Double y) {
        this.y = y;
    }

    public void setK(Double[] k) {
        this.k = k;
    }

    public Double getX() {
        return x;
    }

    public Double getY() {
        return y;
    }

    public Double[] getK() {
        return k;
    }
}
