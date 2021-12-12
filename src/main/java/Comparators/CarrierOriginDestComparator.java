package Comparators;

import WritableComparables.CarrierOriginDest;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CarrierOriginDestComparator extends WritableComparator {
    public CarrierOriginDestComparator() {
        super(CarrierOriginDest.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CarrierOriginDest c1 = (CarrierOriginDest) a;
        CarrierOriginDest c2 = (CarrierOriginDest) b;

        return c1.compareTo(c2);
    }
}
