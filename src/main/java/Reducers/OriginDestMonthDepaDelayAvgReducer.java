package Reducers;

import WritableComparables.OriginDestMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginDestMonthDepaDelayAvgReducer extends Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable> {
    private DoubleWritable avgDepaDeplayDouble = new DoubleWritable();

    @Override
    protected void reduce(OriginDestMonth key, Iterable<DoubleWritable> values, Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double avgDepaDelay = 0.0;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            avgDepaDelay += doubleWritable.get();
            count++;
        }
        avgDepaDelay = avgDepaDelay / count;
        avgDepaDeplayDouble.set(avgDepaDelay);
        context.write(key,avgDepaDeplayDouble);
    }
}
