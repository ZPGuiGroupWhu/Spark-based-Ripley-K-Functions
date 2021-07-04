package org.wysrc.crossK.index.kdbtree;

import org.locationtech.jts.geom.Envelope;
import org.wysrc.crossK.geom.SpatiotemporalEnvelope;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CyclicSplitter implements Serializable {
    private final int X;
    private final int Y;
    private final int Time;
    private Splitter splitter;
    private int currentOrder;
    private AtomicInteger orderBase = new AtomicInteger(0);
    private double splittingPlaneX;
    private double splittingPlaneY;
    private LocalDateTime splittingPlaneTime;

    public CyclicSplitter() {
        this.X = 0;
        this.Y = 1;
        this.Time = 2;
    }

    public CyclicSplitter(int XIndex, int YIndex, int TimeIndex) {
        this.X = XIndex;
        this.Y = YIndex;
        this.Time = TimeIndex;
    }

    private static final class XComparator implements Comparator<ItemEnveloped>, Serializable {
        @Override
        public int compare(ItemEnveloped i1, ItemEnveloped i2) {
            Envelope e1 = ((SpatiotemporalEnvelope) i1.getEnvelope()).spatialEnvelope;
            Envelope e2 = ((SpatiotemporalEnvelope) i2.getEnvelope()).spatialEnvelope;
            return (int) Math.signum(e1.getMinX() - e2.getMinX());
        }
    }

    private static final class YComparator implements Comparator<ItemEnveloped>, Serializable {
        @Override
        public int compare(ItemEnveloped i1, ItemEnveloped i2) {
            Envelope e1 = ((SpatiotemporalEnvelope) i1.getEnvelope()).spatialEnvelope;
            Envelope e2 = ((SpatiotemporalEnvelope) i2.getEnvelope()).spatialEnvelope;
            return (int) Math.signum(e1.getMinY() - e2.getMinY());
        }
    }

    private static final class TimeComparator implements Comparator<ItemEnveloped>, Serializable {
        @Override
        public int compare(ItemEnveloped i1, ItemEnveloped i2) {
            LocalDateTime t1 = ((SpatiotemporalEnvelope) i1.getEnvelope()).startTime;
            LocalDateTime t2 = ((SpatiotemporalEnvelope) i2.getEnvelope()).startTime;
            return t1.compareTo(t2);
        }
    }

    private static final class XSplitter implements Splitter, Serializable {
        private final double x;

        private XSplitter(double x) {
            this.x = x;
        }

        @Override
        public SpatiotemporalEnvelope[] splitEnvelope(SpatiotemporalEnvelope envelope) {
            Envelope spatialEnvelope = envelope.spatialEnvelope;
            LocalDateTime startTime = envelope.startTime;
            LocalDateTime endTime = envelope.endTime;
            SpatiotemporalEnvelope[] envelopes = new SpatiotemporalEnvelope[2];
            envelopes[0] = new SpatiotemporalEnvelope(new Envelope(spatialEnvelope.getMinX(), x, spatialEnvelope.getMinY(), spatialEnvelope.getMaxY()), startTime, endTime);
            envelopes[1] = new SpatiotemporalEnvelope(new Envelope(x, spatialEnvelope.getMaxX(), spatialEnvelope.getMinY(), spatialEnvelope.getMaxY()), startTime, endTime);
            return envelopes;
        }

        @Override
        public List<ItemEnveloped>[] splitItems(List<ItemEnveloped> items) {
            List<ItemEnveloped>[] itemLists = new ArrayList[2];
            itemLists[0] = new ArrayList<>();
            itemLists[1] = new ArrayList<>();
            for(ItemEnveloped item: items) {
                if(((SpatiotemporalEnvelope) item.getEnvelope()).spatialEnvelope.getMinX() <= x) {
                    itemLists[0].add(item);
                } else {
                    itemLists[1].add(item);
                }
            }
            return itemLists;
        }
    }

    private static final class YSplitter implements Splitter, Serializable {
        private final double y;

        private YSplitter(double y) {
            this.y = y;
        }

        @Override
        public SpatiotemporalEnvelope[] splitEnvelope(SpatiotemporalEnvelope envelope) {
            Envelope spatialEnvelope = envelope.spatialEnvelope;
            LocalDateTime startTime = envelope.startTime;
            LocalDateTime endTime = envelope.endTime;
            SpatiotemporalEnvelope[] envelopes = new SpatiotemporalEnvelope[2];
            envelopes[0] = new SpatiotemporalEnvelope(new Envelope(spatialEnvelope.getMinX(), spatialEnvelope.getMaxX(), spatialEnvelope.getMinY(), y), startTime, endTime);
            envelopes[1] = new SpatiotemporalEnvelope(new Envelope(spatialEnvelope.getMinX(), spatialEnvelope.getMaxX(), y, spatialEnvelope.getMaxY()), startTime, endTime);
            return envelopes;
        }

        @Override
        public List<ItemEnveloped>[] splitItems(List<ItemEnveloped> items) {
            List<ItemEnveloped>[] itemLists = new ArrayList[2];
            itemLists[0] = new ArrayList<>();
            itemLists[1] = new ArrayList<>();
            for(ItemEnveloped item: items) {
                if(((SpatiotemporalEnvelope) item.getEnvelope()).spatialEnvelope.getMinY() <= y) {
                    itemLists[0].add(item);
                } else {
                    itemLists[1].add(item);
                }
            }
            return itemLists;
        }
    }

    private static final class TimeSplitter implements Splitter, Serializable {
        private final LocalDateTime time;

        private TimeSplitter(LocalDateTime time) {
            this.time = time;
        }

        @Override
        public SpatiotemporalEnvelope[] splitEnvelope(SpatiotemporalEnvelope envelope) {
            Envelope spatialEnvelope = envelope.spatialEnvelope;
            LocalDateTime startTime = envelope.startTime;
            LocalDateTime endTime = envelope.endTime;
            SpatiotemporalEnvelope[] envelopes = new SpatiotemporalEnvelope[2];
            envelopes[0] = new SpatiotemporalEnvelope(spatialEnvelope, startTime, time);
            envelopes[1] = new SpatiotemporalEnvelope(spatialEnvelope, time, endTime);
            return envelopes;
        }

        @Override
        public List<ItemEnveloped>[] splitItems(List<ItemEnveloped> items) {
            List<ItemEnveloped>[] itemLists = new ArrayList[2];
            itemLists[0] = new ArrayList<>();
            itemLists[1] = new ArrayList<>();
            for(ItemEnveloped item: items) {
                if(((SpatiotemporalEnvelope) item.getEnvelope()).startTime.compareTo(time) <= 0) {
                    itemLists[0].add(item);
                } else {
                    itemLists[1].add(item);
                }
            }
            return itemLists;
        }
    }

    public boolean getSplitter(KDBNode node) {
        if(!node.isLeaf()) {
            currentOrder = node.getSplitOrder();

            if(currentOrder == this.X) {
                splittingPlaneX = node.getSplittingPlaneX();

                SpatiotemporalEnvelope envelope = node.getEnvelope();
                if(! (envelope.spatialEnvelope.getMinX() < splittingPlaneX && envelope.spatialEnvelope.getMaxX() > splittingPlaneX)) {
                    return false;
                }
                splitter = new XSplitter(splittingPlaneX);
                return true;
            } else if(currentOrder == this.Y) {
                splittingPlaneY = node.getSplittingPlaneY();

                SpatiotemporalEnvelope envelope = node.getEnvelope();
                if(! (envelope.spatialEnvelope.getMinY() < splittingPlaneY && envelope.spatialEnvelope.getMaxY() > splittingPlaneY)) {
                    return false;
                }
                splitter = new YSplitter(splittingPlaneY);
                return true;
            } else if(currentOrder == this.Time) {
                splittingPlaneTime = node.getSplittingPlaneTime();

                SpatiotemporalEnvelope envelope = node.getEnvelope();
                if(! (envelope.startTime.compareTo(splittingPlaneTime) < 0 && envelope.endTime.compareTo(splittingPlaneTime) > 0)) {
                    return false;
                }
                splitter = new TimeSplitter(splittingPlaneTime);
                return true;
            }
        } else {
            currentOrder = (node.getSplitOrder()+1) % 3;
            if(currentOrder < 0) {
                currentOrder = orderBase.getAndIncrement() % 3;
            }
            node.setSplitOrder(currentOrder);

            if(currentOrder == this.X) {
                Comparator<ItemEnveloped> comparator = new XComparator();
                List<ItemEnveloped> items = node.getItems();
                items.sort(comparator);
                splittingPlaneX = ((SpatiotemporalEnvelope) items.get(items.size()/2).getEnvelope()).spatialEnvelope.getMinX();

                SpatiotemporalEnvelope envelope = node.getEnvelope();
                if(! (envelope.spatialEnvelope.getMinX() < splittingPlaneX && envelope.spatialEnvelope.getMaxX() > splittingPlaneX)) {
                    return false;
                }
                splitter = new XSplitter(splittingPlaneX);
                return true;
            } else if(currentOrder == this.Y) {
                Comparator<ItemEnveloped> comparator = new YComparator();
                List<ItemEnveloped> items = node.getItems();
                items.sort(comparator);
                splittingPlaneY = ((SpatiotemporalEnvelope) items.get(items.size()/2).getEnvelope()).spatialEnvelope.getMinY();

                SpatiotemporalEnvelope envelope = node.getEnvelope();
                if(! (envelope.spatialEnvelope.getMinY() < splittingPlaneY && envelope.spatialEnvelope.getMaxY() > splittingPlaneY)) {
                    return false;
                }
                splitter = new YSplitter(splittingPlaneY);
                return true;
            } else if(currentOrder == this.Time) {
                Comparator<ItemEnveloped> comparator = new TimeComparator();
                List<ItemEnveloped> items = node.getItems();
                items.sort(comparator);
                splittingPlaneTime = ((SpatiotemporalEnvelope) items.get(items.size()/2).getEnvelope()).startTime;

                SpatiotemporalEnvelope envelope = node.getEnvelope();
                if(! (envelope.startTime.compareTo(splittingPlaneTime) < 0 && envelope.endTime.compareTo(splittingPlaneTime) > 0)) {
                    return false;
                }
                splitter = new TimeSplitter(splittingPlaneTime);
                return true;
            }
        }

        return false;
    }

    public SpatiotemporalEnvelope[] splitEnvelope(SpatiotemporalEnvelope envelope) {
        return this.splitter.splitEnvelope(envelope);
    }

    public List<ItemEnveloped>[] splitItems(List<ItemEnveloped> items) {
        return this.splitter.splitItems(items);
    }

    public int getX() {
        return X;
    }

    public int getY() {
        return Y;
    }

    public int getTime() {
        return Time;
    }

    public double getSplittingPlaneX() {
        return splittingPlaneX;
    }

    public double getSplittingPlaneY() {
        return splittingPlaneY;
    }

    public LocalDateTime getSplittingPlaneTime() {
        return splittingPlaneTime;
    }

    public int getCurrentOrder() {
        return currentOrder;
    }

    public void setCurrentOrder(int currentOrder) {
        this.currentOrder = currentOrder;
    }

    public int getOrderBase() {
        return this.orderBase.get();
    }

    public void setOrderBase(int orderBase) {
        this.orderBase.set(orderBase);
    }
}
