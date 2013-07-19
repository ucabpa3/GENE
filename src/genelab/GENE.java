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
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import sandbox.CombinedInputFormat;
import sandbox.FQInputFormat;
import sandbox.WholeFileInputFormat;

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
        
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        
        Configuration conf= new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length < 2) {
            System.err.println("Usage: GENE  <in> <out>");
            System.exit(2);
        }
        
        Job job = new Job(conf,"align");
        //job.setInputFormatClass(WholeFileInputFormat.class);
        job.setInputFormatClass(CombinedInputFormat.class);

        job.setJarByClass(GENE.class);
        //job.setNumReduceTasks(0);
        job.setMapperClass(BWAIndexMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
       /* JobControl jbcntrl = new JobControl("controller");
        /// Index job
        Configuration indexConf = new Configuration();
        Job index = new Job(indexConf, "index");
        
        index.setInputFormatClass(WholeFileInputFormat.class);

        index.setJarByClass(GENE.class);
        index.setNumReduceTasks(0);
        index.setMapperClass(BWAIndexMapper.class);
        index.setOutputKeyClass(Text.class);
        index.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(index, input);
        FileOutputFormat.setOutputPath(index, output); 
        ControlledJob indexControl = new ControlledJob(indexConf);
        indexControl.setJob(index);
        
        jbcntrl.addJob(indexControl);
        System.out.println("Added job 1");
        ///Align job
        Configuration alignConf = new Configuration();
        Job align = new Job(alignConf, "align");
        
        align.setInputFormatClass(FQInputFormat.class);

        align.setJarByClass(GENE.class);
        align.setMapperClass(BWAMapper.class);
        align.setReducerClass(BWAReducer.class);
        align.setOutputKeyClass(Text.class);
        align.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(align, new Path(args[2]));
        FileOutputFormat.setOutputPath(align, new Path(args[3]));
        ControlledJob alignControl = new ControlledJob(alignConf);
        alignControl.setJob(align);
        System.out.println("Added job 2");
        alignControl.addDependingJob(indexControl);
        
        jbcntrl.addJob(alignControl);
         
        Thread jobControlThread =new Thread(jbcntrl);
        jobControlThread.start();
        
 while( !jbcntrl.allFinished())
    {
      System.out.println("Jobs in waiting state: "
        + jbcntrl.getWaitingJobList().size());
      System.out.println("Jobs in ready state: "
        + jbcntrl.getReadyJobsList().size());
      System.out.println("Jobs in running state: "
        + jbcntrl.getRunningJobList().size());
      System.out.println("Jobs in success state: "
        + jbcntrl.getSuccessfulJobList().size());
      System.out.println("Jobs in failed state: "
        + jbcntrl.getFailedJobList().size());
      //sleep 5 seconds
      try {
        Thread.sleep(5000);
      } catch (Exception e) {  }
    }
 
    //done
    System.exit(0);*/
    }
      
}
