package Reducers;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierMonthDepaDelayMinReducer extends Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable> {
    private DoubleWritable minDepaDelayDouble = new DoubleWritable();

    @Override
    protected void reduce(CarrierMonth key, Iterable<DoubleWritable> values, Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double minDepaDelay = Double.MAX_VALUE;
//        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double min = doubleWritable.get();
            if (min < minDepaDelay) {
                minDepaDelay = min;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        minDepaDelayDouble.set(minDepaDelay);
        context.write(key,minDepaDelayDouble);
    }
}
