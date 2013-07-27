package sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yukun
 * Date: 06/07/2013
 * Time: 01:57
 * To change this template use File | Settings | File Templates.
 */
public class test {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: test <in> <out>");
            System.exit(2);
        }
        conf.set("mapreduce.input.lineinputformat.linespermap", "10");
        Job job = new Job(conf, "test");
        job.setJarByClass(test.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
//        job.setInputFormatClass(NoSplitInputFormat.class);
        job.setInputFormatClass(FQNLineInputFormat2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        private static String v;
        private static String k;

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {

            k= context.getCurrentKey().toString();
//            v=context.getInputSplit()
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("K : " + context.getCurrentKey());
            System.out.println("V : " + context.getCurrentValue());
            v = v + value + "\n";
        }

        @Override
        protected void cleanup(Mapper.Context context)
                throws IOException,
                InterruptedException {

            context.write(new Text(k), new Text(v.substring(0, v.length() - 1)));
        }

    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                for (Text t : values) {
                    System.out.println(t);
                    System.out.println();
                }

        }
    }
}
