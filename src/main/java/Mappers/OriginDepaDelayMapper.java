package Mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OriginDepaDelayMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text originText = new Text();
    private DoubleWritable depaDelayDouble = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] flight_data = value.toString().split(",");
        String origin = flight_data[3];
        if (origin.equals("")) {
            origin = "unknown";
        }
        String depaDelayS = flight_data[7];
        if (depaDelayS.equals("")) {
            depaDelayS = "0.0";
        }
        double depaDelay = Double.parseDouble(depaDelayS);
        originText.set(origin);
        depaDelayDouble.set(depaDelay);

        context.write(originText, depaDelayDouble);
    }
}
