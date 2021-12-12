package Reducers;

import WritableComparables.OriginDestMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginDestMonthDepaDelayMaxReducer extends Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable> {
    private DoubleWritable maxDepaDeplayDouble = new DoubleWritable();

    @Override
    protected void reduce(OriginDestMonth key, Iterable<DoubleWritable> values, Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double maxDepaDelay = Double.MIN_VALUE;
//        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double max = doubleWritable.get();
            if (max > maxDepaDelay) {
                maxDepaDelay = max;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        maxDepaDeplayDouble.set(maxDepaDelay);
        context.write(key,maxDepaDeplayDouble);
    }
}
