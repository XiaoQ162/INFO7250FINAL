package Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginArrDelaySumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable sumArrDeplayDouble = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double sumArrDelay = 0.0;
        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            sumArrDelay += doubleWritable.get();
//            count++;
        }
//        avgDepaDelay = avgDepaDelay / count;
        sumArrDeplayDouble.set(sumArrDelay);
        context.write(key,sumArrDeplayDouble);
    }
}
