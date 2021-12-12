import Comparators.CarrierMonthComparator;
import Comparators.CarrierOriginDestComparator;
import Comparators.OriginDestMonthComparator;
import GroupComparators.CarrierMonthGroupComparator;
import GroupComparators.CarrierOriginDestGroupComparator;
import GroupComparators.OriginDestMonthGroupComparator;
import Mappers.*;
import Partitioners.CarrierMonthPartitioner;
import Partitioners.CarrierOriginDestPartitioner;
import Partitioners.OriginDestMonthPartitioner;
import Reducers.*;
import WritableComparables.CarrierMonth;
import WritableComparables.CarrierOriginDest;
import WritableComparables.OriginDestMonth;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ADAMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"play with secondary sort");

        job.setJarByClass(ADAMain.class);

        job.setGroupingComparatorClass(OriginDestMonthGroupComparator.class);
        job.setSortComparatorClass(OriginDestMonthComparator.class);
        job.setPartitionerClass(OriginDestMonthPartitioner.class);

        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(OriginDestMonth.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(OriginDestMonthArrDelayMapper.class);
//        job.setCombinerClass(.class);
        job.setReducerClass(OriginDestMonthArrDelayMinReducer.class);

        FileInputFormat.addInputPath(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));


        boolean b = job.waitForCompletion(false);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new ADAMain(),args);
        System.exit(exit);

    }
}
