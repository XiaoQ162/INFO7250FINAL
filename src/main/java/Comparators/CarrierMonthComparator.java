package Comparators;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CarrierMonthComparator extends WritableComparator {
    public CarrierMonthComparator() {
        super(CarrierMonth.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CarrierMonth cm1 = (CarrierMonth) a;
        CarrierMonth cm2 = (CarrierMonth) b;

        return cm1.compareTo(cm2);
    }
}
