package Reducers;

import WritableComparables.CarrierOriginDest;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierOriginDestArrDelayMaxReducer extends Reducer<CarrierOriginDest, DoubleWritable, CarrierOriginDest, DoubleWritable> {
    private DoubleWritable maxArrDelayDouble = new DoubleWritable();

    @Override
    protected void reduce(CarrierOriginDest key, Iterable<DoubleWritable> values, Reducer<CarrierOriginDest, DoubleWritable, CarrierOriginDest, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double maxArrDelay = Double.MIN_VALUE;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double max = doubleWritable.get();
            if (max > maxArrDelay) {
                maxArrDelay = max;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        maxArrDelayDouble.set(maxArrDelay);
        context.write(key, maxArrDelayDouble);
    }
}
