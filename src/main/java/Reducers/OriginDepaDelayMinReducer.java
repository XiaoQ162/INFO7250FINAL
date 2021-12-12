package Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginDepaDelayMinReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable minDepaDeplayDouble = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double minDepaDelay = Double.MAX_VALUE;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double curVal = doubleWritable.get();
            if (curVal < minDepaDelay) {
                minDepaDelay = curVal;
            }
        }
        minDepaDeplayDouble.set(minDepaDelay);
        context.write(key,minDepaDeplayDouble);
    }
}
