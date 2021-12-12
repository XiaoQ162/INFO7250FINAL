package Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginDepaDelayMaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable maxDepaDeplayDouble = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double maxDepaDelay = Double.MIN_VALUE;
//        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double curVal = doubleWritable.get();
            if (curVal > maxDepaDelay) {
                maxDepaDelay = curVal;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        maxDepaDeplayDouble.set(maxDepaDelay);
        context.write(key,maxDepaDeplayDouble);
    }
}
