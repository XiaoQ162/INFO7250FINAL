package Reducers;

import WritableComparables.OriginDestMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginDestMonthArrDelayAvgReducer extends Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable> {
    private DoubleWritable avgArrDeplayDouble = new DoubleWritable();

    @Override
    protected void reduce(OriginDestMonth key, Iterable<DoubleWritable> values, Reducer<OriginDestMonth, DoubleWritable, OriginDestMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double avgArrDelay = 0.0;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            avgArrDelay += doubleWritable.get();
            count++;
        }
        avgArrDelay = avgArrDelay / count;
        avgArrDeplayDouble.set(avgArrDelay);
        context.write(key,avgArrDeplayDouble);
    }
}
