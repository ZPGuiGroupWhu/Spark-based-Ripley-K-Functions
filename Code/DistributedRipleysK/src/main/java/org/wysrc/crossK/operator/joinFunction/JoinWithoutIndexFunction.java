package org.wysrc.crossK.operator.joinFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.wysrc.crossK.geom.SpatiotemporalGeometry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JoinWithoutIndexFunction <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry>
    extends JoinFunctionBase
    implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<T, U>>, Serializable {
    public JoinWithoutIndexFunction(boolean includeBoundary) {
        super(includeBoundary);
    }

    @Override
    public Iterator<Pair<T, U>> call(Iterator<T> leftIterator, Iterator<U> rightIterator) {
        List<Pair<T, U>> result = new ArrayList<>();
        if(!leftIterator.hasNext() || !rightIterator.hasNext()) {
            return result.iterator();
        }

        List<U> rightList = new ArrayList<>();
        while(rightIterator.hasNext()) {
            rightList.add(rightIterator.next());
        }

        while(leftIterator.hasNext()) {
            T leftGeometry = leftIterator.next();
            for(U rightGeometry: rightList) {
                if(match(leftGeometry, rightGeometry)) {
                    result.add(Pair.of(leftGeometry, rightGeometry));
                }
            }
        }
        return result.iterator();
    }
}
