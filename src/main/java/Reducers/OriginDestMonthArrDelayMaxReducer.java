package Reducers;

import WritableComparables.OriginDestMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginDestMonthArrDelayMaxReducer extends Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable> {
    private DoubleWritable maxArrDeplayDouble = new DoubleWritable();

    @Override
    protected void reduce(OriginDestMonth key, Iterable<DoubleWritable> values, Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double maxArrDelay = Double.MIN_VALUE;
//        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double max = doubleWritable.get();
            if (max > maxArrDelay) {
                maxArrDelay = max;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        maxArrDeplayDouble.set(maxArrDelay);
        context.write(key,maxArrDeplayDouble);
    }
}
