package genelab;

import BWA.*;
import inputFormat.FQInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import sandbox.FQSplitInfo;

import java.io.IOException;

/**
 * User: yukun
 * Date: 16/07/2013
 * Time: 13:24
 */
public class Main {

    public static void usage() {
        System.err.println("Program: bwa (alignment via Burrows-Wheeler transformation) on Hadoop");
        System.err.println("Version: " + Conf.PACKAGE_VERSION);
        System.err.println("Contact: UCL gene group");
        System.err.println("Usage:   hadoop jar Gen.jar <command> [options]");
        System.err.println("Command: index         index sequences in the FASTA format");
        System.err.println("         mem           BWA-MEM algorithm");
        System.err.println("         backtrack            BWA-backtrack algorithm");
        System.err.println("         clean         clean the files on nodes");
        System.err.println("Note: To use BWA, you need to first index the genome with `bwa index'. There are" +
                " three alignment algorithms in BWA: `mem', `bwasw' and `aln/samse/sampe'. If you are not sure" +
                " which to use, try `bwa mem' first. Please `man ./bwa.1' for for the manual.");
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            Main.usage();
        } else if (args[0].equals("mem")) {
            mem(args);
        } else if (args[0].equals("backtrack")) {
            backtrack(args);
        } else if (args[0].equals("clean")) {
            if (args[1].equals("all")) {
                Maintainer.cleanAll();
            } else if (args[1].equals("reference")) {
                Maintainer.cleanRef();
            } else if (args[1].equals("cache")) {
                Maintainer.cleanCache();
            } else {
                System.err.println("clean [all | reference | cache]");
                System.exit(2);
            }
        } else {
            Main.usage();
        }


    }

    public static void mem(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage:  hadoop jar Gen.jar mem <reference name> <input folder>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.set("reference", args[1]);
        String output = Conf.HDFS_OUTPUT + "mem_" + args[1] + "_" + args[2];
        Job job = new Job(conf, "bwa mem" + Conf.N_LINES_PER_CHUNKS + "lines " + Conf.NUMBER_OF_REDUCERS + "reducers " + args[1] + " " + args[2]);
        job.setJarByClass(Main.class);
        job.setMapperClass(BWAMapper.class);
        job.setReducerClass(BWAMEMReducer.class);
        job.setInputFormatClass(FQInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FQSplitInfo.class);
        job.setNumReduceTasks(Conf.NUMBER_OF_REDUCERS);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(Conf.HDFS_INPUT + args[2]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean exit = job.waitForCompletion(true);
        if (exit) {
            Assistant.merge(output);
            System.exit(0);
        } else {
            System.out.println("job unsuccessful");
            System.exit(1);
        }
    }

    public static void backtrack(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage:  hadoop jar Gen.jar backtrack <reference name> <input folder>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.set("reference", args[1]);
        String output = Conf.HDFS_OUTPUT + "bt_" + args[1] + "_" + args[2];
        Job job = new Job(conf, "bwa backtrack" + Conf.N_LINES_PER_CHUNKS + "lines " + Conf.NUMBER_OF_REDUCERS + "reducers " + args[1] + " " + args[2]);
        job.setJarByClass(Main.class);
        job.setMapperClass(BWAMapper.class);
        job.setReducerClass(BWAbtReducer.class);
        job.setInputFormatClass(FQInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FQSplitInfo.class);
        job.setNumReduceTasks(Conf.NUMBER_OF_REDUCERS);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(Conf.HDFS_INPUT + args[2]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean exit = job.waitForCompletion(true);
        if (exit) {
            Assistant.merge(output);
            System.exit(0);
        } else {
            System.out.println("job unsuccessful");
            System.exit(1);
        }
    }

}
