package Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginDepaDelaySumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable sumDepaDeplayDouble = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double sumDepaDelay = 0.0;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            sumDepaDelay += doubleWritable.get();
//            count++;
        }
//        avgDepaDelay = avgDepaDelay / count;
        sumDepaDeplayDouble.set(sumDepaDelay);
        context.write(key,sumDepaDeplayDouble);
    }
}
