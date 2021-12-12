package Reducers;

import WritableComparables.CarrierOriginDest;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierOriginDestDepaDelayMaxReducer extends Reducer<CarrierOriginDest, DoubleWritable, CarrierOriginDest, DoubleWritable> {
    private DoubleWritable maxDepaDelayDouble = new DoubleWritable();

    @Override
    protected void reduce(CarrierOriginDest key, Iterable<DoubleWritable> values, Reducer<CarrierOriginDest, DoubleWritable, CarrierOriginDest, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double maxDepaDelay = Double.MIN_VALUE;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double max = doubleWritable.get();
            if (max > maxDepaDelay) {
                maxDepaDelay = max;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        maxDepaDelayDouble.set(maxDepaDelay);
        context.write(key, maxDepaDelayDouble);
    }
}
