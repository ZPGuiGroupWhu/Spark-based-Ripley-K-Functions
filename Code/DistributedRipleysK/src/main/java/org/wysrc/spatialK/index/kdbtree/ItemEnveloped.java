package org.wysrc.spatialK.index.kdbtree;

import java.io.Serializable;

public class ItemEnveloped implements Enveloped, Serializable {
    private Object envelope;
    private Object item;

    public ItemEnveloped(Object envelope, Object item) {
        this.envelope = envelope;
        this.item = item;
    }

    @Override
    public Object getEnvelope() {
        return this.envelope;
    }

    public Object getItem() {
        return this.item;
    }
}
