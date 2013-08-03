package genelab;

import BWA.BWAMEMReducer;
import BWA.BWAMapper;
import BWA.BWAbtReducer;
import inputFormat.FQInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import outputForamt.NoKeyOutputFormat;
import sandbox.FQSplitInfo;

import java.io.*;

/**
 * User: yukun
 * Date: 16/07/2013
 * Time: 13:24
 */
public class Main {

    Configuration conf;
    Job job;

    public Main() throws IOException {
        conf = new Configuration();

    }

    public static void usage() {
        System.err.println("Program: bwa (alignment via Burrows-Wheeler transformation) on Hadoop");
        System.err.println("Version: " + Conf.PACKAGE_VERSION);
        System.err.println("Contact: UCL gene group");
        System.err.println("Usage:   hadoop jar Gen.jar <command> [options]");
        System.err.println("Command: index         index sequences in the FASTA format");
        System.err.println("         mem           BWA-MEM algorithm");
        System.err.println("         backtrack     BWA-backtrack algorithm");
        System.err.println("Note: To use BWA, you need to first index the genome with `bwa index'. There are" +
                " three alignment algorithms in BWA: `mem', `bwasw' and `aln/samse/sampe'. If you are not sure" +
                " which to use, try `bwa mem' first. Please `man ./bwa.1' for for the manual.");
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        new Main().mem(args);
        if (args.length < 1) {
            Main.usage();
        }
        if (args[0].equals("mem")) {
            new Main().mem(args);
        }
        if (args[0].equals("backtrak")) {
            new Main().backtrack(args);
        }


    }

    public void mem(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage:  hadoop jar Gen.jar mem <reference name> <input folder>");
            System.exit(2);

        }
        conf.set("reference", args[1]);
        //conf.set("mapred.job.reduce.memory.physical.mb", "6000");
        //conf.set("mapred.job.map.memory.physical.mb", "200");
        String input = "/mapr/mapr-m3-student/myvolume/genelab/input/" + args[2];
        String output = "/mapr/mapr-m3-student/myvolume/genelab/output/" +args[1]+"_"+args[2] ;
//        String input = args[2];
//        String output = args[3];
        job = new Job(conf, "bwa " + Conf.N_LINES_PER_CHUNKS + "lines 12reducers 1processes " + args[1] + " " + args[2]);
        job.setJarByClass(Main.class);
        job.setMapperClass(BWAMapper.class);
        job.setReducerClass(BWAMEMReducer.class);
        job.setInputFormatClass(FQInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FQSplitInfo.class);
        job.setNumReduceTasks(2);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean exit = job.waitForCompletion(true);
        if (exit) {
            merge(output);
        }

        System.exit(exit ? 0 : 1);
    }

    public void merge(String output) throws IOException {

        Configuration conf = new Configuration();
        FileSystem hdfsFileSystem = FileSystem.get(conf);

        Path hdfs = new Path(output + "/temp/");
        FileStatus[] status = hdfsFileSystem.listStatus(hdfs);
        try {
            Path outFile = new Path(output + "/output.bam");
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataOutputStream out = fs.create(outFile);
            for (int i = 1; i <= status.length; i++) {
                FSDataInputStream in = fs.open(new Path(output + "/temp/" + i));
                byte buffer[] = new byte[256];
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();
        } catch (Exception e) {
            System.out.println("File not found");
        }

    }

    public void backtrack(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 4) {
            System.err.println("Usage:  hadoop jar Gen.jar backtrack <reference name> <input folder> <output folder>");
            System.exit(2);
        }
        conf.set("reference", args[1]);
        String input = args[2];
        String output = args[3];
        job = new Job(conf, "bwa on hadoop");
        job.setJarByClass(Main.class);
        job.setMapperClass(BWAMapper.class);
        job.setReducerClass(BWAbtReducer.class);
        job.setInputFormatClass(FQInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(NoKeyOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
