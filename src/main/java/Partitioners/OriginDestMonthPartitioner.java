package Partitioners;

import WritableComparables.OriginDestMonth;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OriginDestMonthPartitioner extends Partitioner<OriginDestMonth, LongWritable> {
    @Override
    public int getPartition(OriginDestMonth originDestMonth, LongWritable longWritable, int i) {
        return originDestMonth.getOrigin().hashCode() % i;
    }
}
