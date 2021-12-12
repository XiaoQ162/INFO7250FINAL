package Reducers;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierMonthArrDelayMinReducer extends Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable> {
    private DoubleWritable minArrDelayDouble = new DoubleWritable();

    @Override
    protected void reduce(CarrierMonth key, Iterable<DoubleWritable> values, Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double minArrDelay = Double.MAX_VALUE;
//        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double min = doubleWritable.get();
            if (min < minArrDelay) {
                minArrDelay = min;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        minArrDelayDouble.set(minArrDelay);
        context.write(key,minArrDelayDouble);
    }
}
