package org.wysrc.crossK.analysis;

import java.io.Serializable;

/**
 * The type of simulation type
 */
public enum SimulationType implements Serializable {
    bootstrapping,

    randomPermutation,

    monteCarlo;

    /**
     * Gets the type of simulation type.
     *
     * @param str the input type name
     * @return the type of simulation type
     */
    public static SimulationType getSimulationType(String str) {
        for(SimulationType type: SimulationType.values()) {
            if(type.name().equalsIgnoreCase(str)) {
                return type;
            }
        }
        return null;
    }
}
