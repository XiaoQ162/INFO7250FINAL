package Mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OriginArrDelayMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text originText = new Text();
    private DoubleWritable arrDelayDouble = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] flight_data = value.toString().split(",");
        String origin = flight_data[3];
        if (origin.equals("")) {
            origin = "unknown";
        }
        String arrDelayS = flight_data[14];
        if (arrDelayS.equals("")) {
            arrDelayS = "0.0";
        }
        double arrDelay = Double.parseDouble(arrDelayS);
        originText.set(origin);
        arrDelayDouble.set(arrDelay);

        context.write(originText, arrDelayDouble);
    }
}
