package Mappers;

import WritableComparables.CarrierOriginDest;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierOriginDestArrDelayMapper extends Mapper<LongWritable, Text, CarrierOriginDest, DoubleWritable> {
    private CarrierOriginDest c = new CarrierOriginDest();
    private DoubleWritable arrDelayDouble = new DoubleWritable();

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
        String arrDelayS = flightRecord[14];
        if (arrDelayS.equals("")) {
            arrDelayS = "0.0";
        }

        c.setCarrier(carrier);
        c.setOrigin(origin);
        c.setDestination(destination);

        arrDelayDouble.set(Double.parseDouble(arrDelayS));
        context.write(c, arrDelayDouble);
    }
}
