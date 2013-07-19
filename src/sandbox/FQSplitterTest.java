package sandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FQSplitterTest {

    public static class FQSplitterMapper extends Mapper<LongWritable, Text, Integer, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("key: " + key);
            System.out.println(value);
        }
    }

    public static class FQSplitterReducer extends Reducer<Integer, Text, Integer, Text> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Integer key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(FQSplitterTest.class);
        job.setMapperClass(FQSplitterMapper.class);
        job.setCombinerClass(FQSplitterReducer.class);
        job.setReducerClass(FQSplitterReducer.class);
        job.setInputFormatClass(FQInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}