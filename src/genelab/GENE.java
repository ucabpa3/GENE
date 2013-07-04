/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package genelab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author costas
 */
public class GENE{

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        // TODO code application logic here
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length < 2) {
            System.err.println("Usage: GENE  <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "align");
        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(GENE.class);
        job.setMapperClass(BWAMapper.class);
        job.setReducerClass(BWAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

      

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
