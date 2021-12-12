package Partitioners;

import WritableComparables.CarrierMonth;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CarrierMonthPartitioner extends Partitioner<CarrierMonth, LongWritable> {
    @Override
    public int getPartition(CarrierMonth carrierMonth, LongWritable longWritable, int i) {
        return carrierMonth.getCarrier().hashCode() % i;
    }
}
