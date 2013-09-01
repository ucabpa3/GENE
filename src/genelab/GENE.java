/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package genelab;

import hadoop.mapper.BWAIndexMapper;
import hadoop.mapper.BWAMapper;
import hadoop.reducer.BWAReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author costas
 */
public class GENE {

    /**
     * @param args the command line arguments
     */

    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        
        /*String[] otherArgs = new GenericOptionsParser(alignConf, args).getRemainingArgs();
        
        if (otherArgs.length < 2) {
            System.err.println("Usage: GENE  <in> <out>");
            System.exit(2);
        }*/

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        JobControl jbcntrl = new JobControl("controller");
        System.out.println("What the hell?");
        /// Index job
        Configuration indexConf = new Configuration();
        Job index = new Job(indexConf, "index");

        // index.setInputFormatClass(WholeFileInputFormat.class);

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

        ///Align job
        Configuration alignConf = new Configuration();
        Job align = new Job(alignConf, "align");


        align.setJarByClass(GENE.class);
        align.setMapperClass(BWAMapper.class);
        align.setReducerClass(BWAReducer.class);
        align.setOutputKeyClass(Text.class);
        align.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(align, new Path(args[2]));
        FileOutputFormat.setOutputPath(align, new Path(args[3]));
        ControlledJob alignControl = new ControlledJob(alignConf);
        alignControl.setJob(align);
        alignControl.addDependingJob(indexControl);

        jbcntrl.addJob(alignControl);

        Thread jobControlThread = new Thread(jbcntrl);
        jobControlThread.start();

        while (!jbcntrl.allFinished()) {
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
            } catch (Exception e) {
            }
        }

        //done
        System.exit(0);
    }

}
