package org.whu.geoai_stval.spark_K_functions.space_K.operator.joinFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.whu.geoai_stval.spark_K_functions.space_K.geom.SpatiotemporalGeometry;
import org.whu.geoai_stval.spark_K_functions.space_K.index.SpatiotemporalIndex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JoinWithLeftIndexFunction <T extends SpatiotemporalGeometry, U extends SpatiotemporalGeometry>
    extends JoinFunctionBase
    implements FlatMapFunction2<Iterator<SpatiotemporalIndex>, Iterator<U>, Pair<T, U>>, Serializable {

    public JoinWithLeftIndexFunction(boolean includeBoundary) {
        super(includeBoundary);
    }

    @Override
    public Iterator<Pair<T, U>> call(Iterator<SpatiotemporalIndex> indexIterator, Iterator<U> geometryIterator) {
        List<Pair<T, U>> result = new ArrayList<>();
        if(!indexIterator.hasNext() || !geometryIterator.hasNext()) {
            return result.iterator();
        }

        SpatiotemporalIndex index = indexIterator.next();
        while(geometryIterator.hasNext()) {
            U geometry = geometryIterator.next();
            List<T> candidates = index.query(geometry.getEnvelopeInternal());
            for(T candidate: candidates) {
                if(match(candidate, geometry)) {
                    result.add(Pair.of(candidate, geometry));
                }
            }
        }
        return result.iterator();
    }
}
