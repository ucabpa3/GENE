package sandbox;

import BWA.BWAMapper;
import BWA.BWAbtReducer;
import inputFormat.FQInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import outputForamt.NoKeyOutputFormat;

/**
 * User: yukun
 * Date: 16/07/2013
 * Time: 12:46
 */
public class BWAbtTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: BWAMEMTest <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "bwa-bt");
        job.setJarByClass(BWAbtTest.class);
        job.setMapperClass(BWAMapper.class);
        job.setReducerClass(BWAbtReducer.class);
        job.setInputFormatClass(FQInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(NoKeyOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
