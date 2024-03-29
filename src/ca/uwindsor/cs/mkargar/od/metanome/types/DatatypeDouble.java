package ca.uwindsor.cs.mkargar.od.metanome.types;

/**
 * Created by Mehdi on 7/1/2016.
 */
import java.util.Comparator;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import ca.uwindsor.cs.mkargar.od.metanome.sorting.partitions.RowIndexedDoubleValue;

public class DatatypeDouble extends Datatype {

    public DatatypeDouble(final Comparator<Double> comparator) {
        this.specificType = type.DOUBLE;

        if (comparator == null) {
            this.indexedComparator = new Comparator<RowIndexedDoubleValue>() {

                @Override
                public int compare(final RowIndexedDoubleValue o1, final RowIndexedDoubleValue o2) {
                    return ComparisonChain.start()
                            .compare(o1.value, o2.value, Ordering.natural().nullsFirst()).result();
                }

            };
        } else {
            this.indexedComparator = new Comparator<RowIndexedDoubleValue>() {

                @Override
                public int compare(final RowIndexedDoubleValue o1, final RowIndexedDoubleValue o2) {
                    return ComparisonChain.start()
                            .compare(o1.value, o2.value, Ordering.from(comparator).nullsFirst()).result();
                }

            };
        }

    }
}

