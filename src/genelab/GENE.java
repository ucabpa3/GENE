/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package genelab;

import BWA.BWAIndexMapper;
import BWA.BWAMapper;
import BWA.BWAReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import sandbox.FQInputFormat;

/**
 *
 * @author costas
 */
public class GENE{

    /**
     * @param args the command line arguments
     */
    
    private static Configuration conf;
    
    public static void main(String[] args) throws Exception{
        // TODO code application logic here
        
        String[] otherArgs = new GenericOptionsParser(GENE.conf, args).getRemainingArgs();
        
        if (otherArgs.length < 2) {
            System.err.println("Usage: GENE  <in> <out>");
            System.exit(2);
        }
        Path input = new Path(otherArgs[0]);
        Path output = new Path(otherArgs[1]);
        align(input,output);
    }

    public GENE() {
        GENE.conf = new Configuration();
    }
    private static void align(Path input, Path output) throws Exception{
        
        Job job = new Job(GENE.conf, "align");
        
        job.setInputFormatClass(FQInputFormat.class);

        job.setJarByClass(GENE.class);
        job.setMapperClass(BWAMapper.class);
        job.setReducerClass(BWAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);   

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
