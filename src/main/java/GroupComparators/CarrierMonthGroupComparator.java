package GroupComparators;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CarrierMonthGroupComparator extends WritableComparator {
    public CarrierMonthGroupComparator() {
        super(CarrierMonth.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CarrierMonth c1 = (CarrierMonth) a;
        CarrierMonth c2 = (CarrierMonth) b;

        return c1.compareTo(c2);
    }

    @Override
    public int compare(Object a, Object b) {
        CarrierMonth c1 = (CarrierMonth) a;
        CarrierMonth c2 = (CarrierMonth) b;

        return c1.compareTo(c2);
    }
}
