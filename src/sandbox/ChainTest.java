package sandbox;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: yukun
 * Date: 24/07/2013
 * Time: 16:27
 */
public class ChainTest   {
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(ChainTest.class);
        conf.setJobName("Chain");

        JobConf mapperConf = new JobConf();



        ChainReducer chainReducer = new ChainReducer();
//        chainReducer.addReducer(conf, MyReducer.class, LongWritable.class, Text.class, LongWritable.class, Text.class, false, mapperConf);
//        chainReducer.addReducer(conf, otherMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, false, mapperConf);


//Input and output format
        conf.setOutputFormat(NullOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("/user/yukun/input/a.fq"), new Path("/user/yukun/input/b.fq"));
        JobClient.runJob(conf);
    }

    public static class MyMapper implements Mapper<LongWritable, Text, LongWritable, Text> {


        @Override
        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> collector, Reporter reporter) throws IOException {
            System.out.println("key: "+key);
            System.out.println("value: "+value);
            System.out.println("~~~~~~~~~~~~~~~~~~");
            collector.collect(key,value);
        }

        @Override
        public void close() throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void configure(JobConf entries) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }


    public static class MyReducer implements Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        public void reduce(IntWritable intWritable, Iterator<Text> textIterator, OutputCollector<Text, IntWritable> textIntWritableOutputCollector, Reporter reporter) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void close() throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void configure(JobConf entries) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
