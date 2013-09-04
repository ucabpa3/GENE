package hadoop;

import genelab.Conf;
import inputFormat.FQInputFormat;
import inputFormat.FQSplitInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * User: yukun
 * Date: 29/08/2013
 * Time: 17:45
 */
public class AlignmentPrepossess {
    public static boolean run(String input) throws Exception {
        Configuration conf = new Configuration();
        conf.set("input", input);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(Conf.HDFS_INDEX + input))) {
            fs.delete(new Path(Conf.HDFS_INDEX + input), true);
        }
        Job job = new Job(conf, "bwa prepossess " + input);
        job.setJarByClass(AlignmentPrepossess.class);
        job.setMapperClass(BWASplitMapper.class);
        job.setReducerClass(BWASplitReducer.class);
        job.setInputFormatClass(FQInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(Conf.HDFS_INPUT + input));
        if (job.waitForCompletion(true)) {
            fs.delete(new Path(Conf.HDFS_INDEX + input + "/log"), true);
            return true;
        } else {
            return false;
        }
    }

    public static class BWASplitMapper extends Mapper<LongWritable, FQSplitInfo, LongWritable, Text> {

        public void map(LongWritable key, FQSplitInfo value, Context context) throws IOException, InterruptedException {
            System.out.println(value);
            Text info = new Text(value.toString());
            context.write(key, info);
        }

    }

    public static class BWASplitReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        FSDataOutputStream out;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path indexFile = new Path(Conf.HDFS_INDEX + context.getConfiguration().get("input") + "/index");
            out = fs.create(indexFile, true);
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            out.writeBytes(key.get() + "\n");
            int n = 0;
            for (Text info : values) {
                n++;
                out.writeBytes(info.toString() + "\n");
            }
            //if the data is single, write an empty line
            if (n == 1) {
                out.writeBytes("\b");
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            out.close();
        }
    }
}
