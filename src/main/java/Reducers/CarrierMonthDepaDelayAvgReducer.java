package Reducers;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierMonthDepaDelayAvgReducer extends Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable> {
    private DoubleWritable avgDepaDelayDouble = new DoubleWritable();

    @Override
    protected void reduce(CarrierMonth key, Iterable<DoubleWritable> values, Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double avgDepaDelay = 0.0;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            avgDepaDelay += doubleWritable.get();
            count++;
        }
        avgDepaDelay = avgDepaDelay / count;
        avgDepaDelayDouble.set(avgDepaDelay);
        context.write(key,avgDepaDelayDouble);
    }
}
