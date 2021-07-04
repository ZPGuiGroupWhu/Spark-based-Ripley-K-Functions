package org.wysrc.spatialK.operator.joinFunction;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wysrc.spatialK.geom.SpatiotemporalGeometry;
import org.wysrc.spatialK.geom.SpatiotemporalPoint;

import java.io.Serializable;

public class JoinFunctionBase implements Serializable {
    private static final Logger log = LogManager.getLogger(JoinFunctionBase.class);

    private final boolean includeBoundary;

    protected JoinFunctionBase(boolean includeBoundary) {
        this.includeBoundary = includeBoundary;
    }

    protected boolean match(SpatiotemporalGeometry left, SpatiotemporalGeometry right) {
        if(left instanceof SpatiotemporalPoint) {
            return includeBoundary? right.intersects(left): right.contains(left);
        } else {
            return includeBoundary? left.intersects(right): left.contains(right);
        }
    }

    protected boolean getIncludeBoundary() {
        return includeBoundary;
    }
}
