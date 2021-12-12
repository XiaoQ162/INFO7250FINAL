package GroupComparators;

import WritableComparables.OriginDestMonth;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OriginDestMonthGroupComparator extends WritableComparator {
    public OriginDestMonthGroupComparator() {
        super(OriginDestMonth.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OriginDestMonth o1 = (OriginDestMonth) a;
        OriginDestMonth o2 = (OriginDestMonth) b;

        return o1.compareTo(o2);
    }

    @Override
    public int compare(Object a, Object b) {
        OriginDestMonth o1 = (OriginDestMonth) a;
        OriginDestMonth o2 = (OriginDestMonth) b;

        return o1.compareTo(o2);
    }
}
