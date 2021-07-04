package org.wysrc.localK.operator.joinFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.wysrc.localK.geom.SpatiotemporalGeometry;
import org.wysrc.localK.index.SpatiotemporalIndex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JoinWithRightIndexFunction <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry>
    extends JoinFunctionBase
    implements FlatMapFunction2<Iterator<T>, Iterator<SpatiotemporalIndex>, Pair<T, U>>, Serializable {

    public JoinWithRightIndexFunction(boolean includeBoundary) {
        super(includeBoundary);
    }

    @Override
    public Iterator<Pair<T, U>> call(Iterator<T> geometryIterator, Iterator<SpatiotemporalIndex> indexIterator) {
        List<Pair<T, U>> result = new ArrayList<>();
        if(!geometryIterator.hasNext() || !indexIterator.hasNext()) {
            return result.iterator();
        }

        SpatiotemporalIndex index = indexIterator.next();
        while(geometryIterator.hasNext()) {
            T geometry = geometryIterator.next();
            List<U> candidates = index.query(geometry.getEnvelopeInternal());
            for(U candidate: candidates) {
                if(match(geometry, candidate)) {
                    result.add(Pair.of(geometry, candidate));
                }
            }
        }
        return result.iterator();
    }
}
