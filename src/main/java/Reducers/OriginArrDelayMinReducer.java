package Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginArrDelayMinReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable minArrDeplayDouble = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double minArrDelay = Double.MAX_VALUE;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double curVal = doubleWritable.get();
            if (curVal < minArrDelay) {
                minArrDelay = curVal;
            }
        }
        minArrDeplayDouble.set(minArrDelay);
        context.write(key,minArrDeplayDouble);
    }
}
