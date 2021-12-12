package Reducers;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierMonthArrDelayMaxReducer extends Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable> {
    private DoubleWritable maxArrDelayDouble = new DoubleWritable();

    @Override
    protected void reduce(CarrierMonth key, Iterable<DoubleWritable> values, Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double maxArrDelay = Double.MIN_VALUE;
//        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double max = doubleWritable.get();
            if (max > maxArrDelay) {
                maxArrDelay = max;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        maxArrDelayDouble.set(maxArrDelay);
        context.write(key,maxArrDelayDouble);
    }
}
