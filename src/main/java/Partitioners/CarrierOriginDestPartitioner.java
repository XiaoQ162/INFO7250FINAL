package Partitioners;

import WritableComparables.CarrierOriginDest;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CarrierOriginDestPartitioner extends Partitioner<CarrierOriginDest, LongWritable> {
    @Override
    public int getPartition(CarrierOriginDest carrierOriginDest, LongWritable longWritable, int i) {
        return carrierOriginDest.getCarrier().hashCode() % i;
    }
}
