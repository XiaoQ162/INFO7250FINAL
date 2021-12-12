package Mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierArrDelayMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text carrierText = new Text();
    private DoubleWritable arrDelayDouble = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] flight_data = value.toString().split(",");
        String carrier = flight_data[1];
        if (carrier.equals("")) {
            carrier = "unknown";
        }
        String arrDelayS = flight_data[14];
        if (arrDelayS.equals("")) {
            arrDelayS = "0.0";
        }
        double arrDelay = Double.parseDouble(arrDelayS);
        carrierText.set(carrier);
        arrDelayDouble.set(arrDelay);

        context.write(carrierText, arrDelayDouble);
    }
}
