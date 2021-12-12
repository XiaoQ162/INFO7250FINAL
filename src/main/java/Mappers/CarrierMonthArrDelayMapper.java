package Mappers;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierMonthArrDelayMapper extends Mapper<LongWritable, Text, CarrierMonth, DoubleWritable> {
    private CarrierMonth cm = new CarrierMonth();
    private DoubleWritable arrDelayDouble = new DoubleWritable();

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

        String arrDelayS = flightRecord[14];
        if (arrDelayS.equals("")) {
            arrDelayS = "0.0";
        }

        cm.setCarrier(carrier);
        cm.setMonth(monthS);

        arrDelayDouble.set(Double.parseDouble(arrDelayS));
        context.write(cm, arrDelayDouble);
    }
}
