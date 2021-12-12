package Reducers;

import WritableComparables.OriginDestMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginDestMonthArrDelayMinReducer extends Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable> {
    private DoubleWritable minArrDeplayDouble = new DoubleWritable();

    @Override
    protected void reduce(OriginDestMonth key, Iterable<DoubleWritable> values, Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double minArrDelay = Double.MAX_VALUE;
//        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double min = doubleWritable.get();
            if (min < minArrDelay) {
                minArrDelay = min;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        minArrDeplayDouble.set(minArrDelay);
        context.write(key,minArrDeplayDouble);
    }
}
