package Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierDepaDelayAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable avgDepaDeplayDouble = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
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
