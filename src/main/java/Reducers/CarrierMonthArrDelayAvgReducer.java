package Reducers;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierMonthArrDelayAvgReducer extends Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable> {
    private DoubleWritable avgArrDelayDouble = new DoubleWritable();

    @Override
    protected void reduce(CarrierMonth key, Iterable<DoubleWritable> values, Reducer<CarrierMonth, DoubleWritable, CarrierMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double avgArrDelay = 0.0;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            avgArrDelay += doubleWritable.get();
            count++;
        }
        avgArrDelay = avgArrDelay / count;
        avgArrDelayDouble.set(avgArrDelay);
        context.write(key,avgArrDelayDouble);
    }
}
