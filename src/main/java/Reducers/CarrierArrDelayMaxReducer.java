package Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierArrDelayMaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable maxArrDeplayDouble = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double maxArrDelay = Double.MIN_VALUE;
//        int count = 0;
        for (DoubleWritable doubleWritable : values) {
            double curVal = doubleWritable.get();
            if (curVal > maxArrDelay) {
                maxArrDelay = curVal;
            }
        }
//        avgDepaDelay = avgDepaDelay / count;
        maxArrDeplayDouble.set(maxArrDelay);
        context.write(key,maxArrDeplayDouble);
    }
}
