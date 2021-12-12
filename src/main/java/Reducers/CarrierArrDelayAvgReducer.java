package Reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierArrDelayAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable avgArrDeplayDouble = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
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
