package Mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierDepaDelayMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text carrierText = new Text();
    private DoubleWritable depaDelayDouble = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] flight_data = value.toString().split(",");
        String carrier = flight_data[1];
        if (carrier.equals("")) {
            carrier = "unknown";
        }
        String depaDelayS = flight_data[7];
        if (depaDelayS.equals("")) {
            depaDelayS = "0.0";
        }
        double depaDelay = Double.parseDouble(depaDelayS);
        carrierText.set(carrier);
        depaDelayDouble.set(depaDelay);

        context.write(carrierText, depaDelayDouble);
    }
}
