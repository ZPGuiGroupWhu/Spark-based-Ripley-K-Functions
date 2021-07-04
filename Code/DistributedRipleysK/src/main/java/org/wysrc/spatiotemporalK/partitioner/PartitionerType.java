package org.wysrc.spatiotemporalK.partitioner;

import java.io.Serializable;

/**
 *  The type of partitioner
 */
public enum PartitionerType implements Serializable {
    /**
     * Partitioner type that divides range Equally
     */
    Equal,

    /**
     * Partitioner type based on OcTree
     */
    OcTree,

    /**
     * Partitioner type based on STRTree
     */
    STRTree,

    /**
     * Partitioner type based on KDBTree
     */
    KDBTree,

    /**
     * Partitioner type based on existing cuboids
     */
    Cuboid;

    /**
     * Gets the type of partitioner
     *
     * @param str input type name
     * @return the type of partitioner
     */
    public static PartitionerType getPartitionerType(String str) {
        for(PartitionerType type: PartitionerType.values()) {
            if(type.name().equalsIgnoreCase(str)) {
                return type;
            }
        }
        return null;
    }
}
