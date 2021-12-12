package GroupComparators;

import WritableComparables.CarrierOriginDest;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CarrierOriginDestGroupComparator extends WritableComparator {
    public CarrierOriginDestGroupComparator() {
        super(CarrierOriginDest.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CarrierOriginDest c1 = (CarrierOriginDest) a;
        CarrierOriginDest c2 = (CarrierOriginDest) b;

        return c1.compareTo(c2);
    }

    @Override
    public int compare(Object a, Object b) {
        CarrierOriginDest c1 = (CarrierOriginDest) a;
        CarrierOriginDest c2 = (CarrierOriginDest) b;

        return c1.compareTo(c2);
    }
}
