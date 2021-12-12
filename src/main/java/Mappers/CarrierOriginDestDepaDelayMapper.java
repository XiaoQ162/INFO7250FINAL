package Mappers;

import WritableComparables.CarrierOriginDest;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierOriginDestDepaDelayMapper extends Mapper<LongWritable, Text, CarrierOriginDest, DoubleWritable> {
    private CarrierOriginDest c = new CarrierOriginDest();
    private DoubleWritable depaDelayDouble = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CarrierOriginDest, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] flightRecord = value.toString().split(",");
        String carrier = flightRecord[1];
        if (carrier.equals("")) {
            carrier = "unknown";
        }
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

        c.setCarrier(carrier);
        c.setOrigin(origin);
        c.setDestination(destination);

        depaDelayDouble.set(Double.parseDouble(depaDelayS));
        context.write(c, depaDelayDouble);
    }
}
