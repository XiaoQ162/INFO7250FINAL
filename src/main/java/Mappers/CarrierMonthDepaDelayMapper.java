package Mappers;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierMonthDepaDelayMapper extends Mapper<LongWritable, Text, CarrierMonth, DoubleWritable> {
    private CarrierMonth cm = new CarrierMonth();
    private DoubleWritable depaDelayDouble = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CarrierMonth, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] flightRecord = value.toString().split(",");
        String carrier = flightRecord[1];
        if (carrier.equals("")) {
            carrier = "unknown";
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

        String depaDelayS = flightRecord[7];
        if (depaDelayS.equals("")) {
            depaDelayS = "0.0";
        }

        cm.setCarrier(carrier);
        cm.setMonth(monthS);

        depaDelayDouble.set(Double.parseDouble(depaDelayS));
        context.write(cm, depaDelayDouble);
    }
}
