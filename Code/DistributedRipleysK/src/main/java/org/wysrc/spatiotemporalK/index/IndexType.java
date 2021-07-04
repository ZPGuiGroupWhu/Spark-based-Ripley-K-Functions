package org.wysrc.spatiotemporalK.index;

import java.io.Serializable;

/**
 * The type of indexes
 */
public enum IndexType implements Serializable {
    /**
     * Index based on STRTree
     */
    STRTree,

    /**
     * Index based on KDBTree
     */
    KDBTree;

    /**
     * Get the type of index
     *
     * @param str input type name
     * @return the type of index
     */
    public static IndexType getIndexType(String str) {
        for(IndexType type: IndexType.values()) {
            if(type.name().equalsIgnoreCase(str)) {
                return type;
            }
        }
        return null;
    }
}
