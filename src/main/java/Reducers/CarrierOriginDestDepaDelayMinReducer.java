package Reducers;

import WritableComparables.CarrierOriginDest;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierOriginDestDepaDelayMinReducer extends Reducer<CarrierOriginDest, DoubleWritable, CarrierOriginDest, DoubleWritable> {
    private DoubleWritable minDepaDelayDouble = new DoubleWritable();

    @Override
    protected void reduce(CarrierOriginDest key, Iterable<DoubleWritable> values, Reducer<CarrierOriginDest, DoubleWritable, CarrierOriginDest, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double minDepaDelay = Double.MAX_VALUE;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double min = doubleWritable.get();
            if (min < minDepaDelay) {
                minDepaDelay = min;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        minDepaDelayDouble.set(minDepaDelay);
        context.write(key, minDepaDelayDouble);
    }
}
