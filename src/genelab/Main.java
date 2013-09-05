package genelab;

import hadoop.AlignmentPrepossess;
import hadoop.Maintainer;
import hadoop.mapper.AlignmentMapper;
import hadoop.reducer.AlignmentReducer;
import inputFormat.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import hadoop.mapper.BWAIndexMapper;

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
        } else if (args[0].equals("mem") || args[0].equals("backtrack")) {
//            AlignmentPrepossess.run(args[2]);
            align(args);
        }
        else if(args[0].equals("index"))  {
               index(args[1]);
        }
        else if (args[0].equals("clean")) {
            if (args[1].equals("all")) {
                Maintainer.cleanAll();
            } else if (args[1].equals("reference")) {
                Maintainer.cleanRef();
            } else if (args[1].equals("cache")) {
                Maintainer.cleanCache();
            } else if (args[1].equals("bwa")) {
                Maintainer.cleanBWA();
            } else {
                System.err.println("clean [all | baw ｜ reference | cache]");
                System.exit(2);
            }
        } else {
            Main.usage();
        }
    }

    public static void align(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage:  hadoop jar Gen.jar mem <reference name> <input folder>");
            System.exit(2);
        }
        String output = Conf.HDFS_OUTPUT +args[0]+ "_" + args[1] + "_" + args[2];
        Configuration conf = new Configuration();
        conf.set("algorithm", args[0]);
        conf.set("reference", args[1]);
        conf.set("outputPath", output);
        conf.set("mapreduce.input.lineinputformat.linespermap", "3");
        conf.set("mapreduce.tasktracker.reserved.physicalmemory.mb", Conf.RESERVED_MEMORY);
        conf.set("mapred.tasktracker.map.tasks.maximum", "1");
        conf.set("mapreduce.map.java.opts", "-Xmx9000m");
        Job job = new Job(conf, "bwa " + args[0] + " " + Conf.N_LINES_PER_CHUNKS + "lines " + args[1] + " " + args[2]);
        job.setJarByClass(Main.class);
        job.setMapperClass(AlignmentMapper.class);
        job.setReducerClass(AlignmentReducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(Conf.HDFS_INDEX + args[2]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(output), true);
        fs.mkdirs(new Path(output + "/temp/"));
        fs.createNewFile(new Path(output + "/result/0"));
        boolean exit = job.waitForCompletion(true);
        if (exit) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

    public static void index(String refName) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("input",refName);
        Job job = new Job(conf, refName+"alignment");
        job.setJarByClass(Main.class);
        job.setMapperClass(BWAIndexMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(refName));
        FileOutputFormat.setOutputPath(job, new Path("indexed_output"));

        boolean exit = job.waitForCompletion(true);
        if (exit) {
            System.exit(0);
        } else {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("indexed_output"), true);
            System.exit(1);
        }
    }


}
