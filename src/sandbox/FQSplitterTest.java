package sandbox;

import genelab.Conf;
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

import java.io.*;
import java.util.Arrays;

public class FQSplitterTest {

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

        job.setReducerClass(FQSplitterReducer.class);
        job.setInputFormatClass(FQInputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class FQSplitterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("key: " + key);
//            System.out.println("value: " + value);
            context.write(key, value);
        }
    }

    public static class FQSplitterReducer extends Reducer<LongWritable, Text, String, String> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            File workingDir = new File(Conf.PATH_MAIN + context.getJobID().toString());
            //create working folder
            System.out.println(workingDir.getAbsolutePath());
            if (new File(workingDir.getAbsolutePath()).mkdir()) {
                System.out.println("created the local working directory");
            }

            //write down .fq files
            String inputPath[] = new String[2];
            for (Text v : value) {
                String in = v.toString();
                String name = in.split("\n")[0];
                String inPath = workingDir.getAbsolutePath() + "/" + name + "_" + key + ".fq";

                if (inputPath[0] == null) {
                    inputPath[0] = inPath;
                } else {
                    inputPath[1] = inPath;

                }
                System.out.println("inPath: " + inPath);
                File file = new File(inPath);
                if (!file.exists()) {
                    file.createNewFile();
                    FileWriter fw = new FileWriter(file.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);
                    bw.write(in, (name + "\n").length(), in.length() - (name + "\n").length());
                    bw.close();
                }
            }
            Arrays.sort(inputPath);

            //start to run command
            String bwa = Conf.PATH_BWA + "bwa";
            String command = bwa + " " + "mem" + " " + Conf.PATH_REFERENCE + "reference.fa "
                    + " " + inputPath[0] + " " + inputPath[1];
            System.out.println("command :" + command);
            Process p = Runtime.getRuntime().exec(command);

            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            InputStream er = p.getErrorStream();
            InputStreamReader err = new InputStreamReader(er);
            BufferedReader br_err = new BufferedReader(err);
            String line;
            String error;
            String output = "";
            while ((line = br.readLine()) != null) {
                //Outputs your process execution
//                System.out.println("Line:" + line);
                if (!(line.substring(0, 1)).equals("@") && key.toString().equals("1")) {
                    output = output + line + "\n";
                }
            }

            while ((error = br_err.readLine()) != null) {
                //Outputs your process execution
                System.out.println("Terminal: " + error);
            }

            OutputStream outputStream = p.getOutputStream();
            PrintStream printStream = new PrintStream(outputStream);
            printStream.println();
            printStream.flush();
            printStream.close();
            System.out.println(output.length());
            System.out.println(output.length() - "\n".length());
            context.write("", output.substring(0, output.length() - "\n".length()));
        }
    }

}
