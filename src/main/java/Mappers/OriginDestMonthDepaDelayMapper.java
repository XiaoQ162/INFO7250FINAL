package Mappers;

import WritableComparables.OriginDestMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OriginDestMonthDepaDelayMapper extends Mapper<LongWritable, Text, OriginDestMonth, DoubleWritable> {
    private OriginDestMonth o = new OriginDestMonth();
    private DoubleWritable depaDelayDouble = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, OriginDestMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] flightRecord = value.toString().split(",");
        String origin = flightRecord[3];
        if (origin.equals("")) {
            origin = "unknown";
        }
        String destination = flightRecord[4];
        if (destination.equals("")) {
            destination = "unknown";
        }
        String depaDelayS = flightRecord[7];
        if (depaDelayS.equals("")) {
            depaDelayS = "0.0";
        }
        String date = flightRecord[0];
        if (date.equals("")) {
            date = "0000-00-00";
        }
        String[] dateArr = date.split("-");
        String monthS = dateArr[1];
        if (monthS.equals("")) {
            monthS = "0";
        }

        o.setOrigin(origin);
        o.setDestination(destination);
        o.setMonth(monthS);

        depaDelayDouble.set(Double.parseDouble(depaDelayS));

        context.write(o, depaDelayDouble);

    }
}
