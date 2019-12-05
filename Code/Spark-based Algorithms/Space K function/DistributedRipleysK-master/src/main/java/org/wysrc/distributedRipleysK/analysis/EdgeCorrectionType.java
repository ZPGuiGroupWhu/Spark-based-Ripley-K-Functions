package org.wysrc.distributedRipleysK.analysis;

import java.io.Serializable;

/**
 * The type of edge correction method
 */
public enum EdgeCorrectionType implements Serializable {
    border,

    isotropic,

    translation;

    /**
     * Gets the type of edge correction method
     *
     * @param str input type name
     * @return the type of edge correction method
     */
    public static EdgeCorrectionType getEdgeCorrectionType(String str) {
        for(EdgeCorrectionType type: EdgeCorrectionType.values()) {
            if(type.name().equalsIgnoreCase(str)) {
                return type;
            }
        }
        return null;
    }
}
