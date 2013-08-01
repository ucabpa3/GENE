package BWA;

import genelab.Conf;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org .apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.*;
import java.util.Arrays;

/**
 * User: yukun
 * Date: 12/07/2013
 * Time: 14:02
 */

public class BWAMEMReducer extends Reducer<LongWritable, Text, String, String> {

    MultipleOutputs mos = null;

    @Override
    protected void setup(Context context){
            mos = new MultipleOutputs(context);
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        //FileSystem hdfsFileSystem = FileSystem.get(conf);
        File workingDir = new File(Conf.PATH_MAIN + context.getJobID().toString() + "_" + key);
        System.out.println(workingDir.getAbsolutePath());

        if (new File(workingDir.getAbsolutePath()).mkdirs()) {
            System.out.println("created the local working directory");
        }

        Assistant.copyReference(context.getConfiguration());
        Assistant.copyBWA(context.getConfiguration());

        this.runCommand("rm -r job_*");

//        write down .fq files
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
//            System.out.println("inPath: " + inPath);
            File file = new File(inPath);
            if (!file.exists()) {
                file.createNewFile();
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(in, (name + "\n").length(), in.length() - (name + "\n").length());
                bw.close();
            }
        }

        //start to run command
        String bwa = Conf.PATH_BWA + "bwa";
        String command;
        if (inputPath[1] == null || inputPath.equals("")) {
            command = bwa + " mem " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa "
                    + " " + inputPath[0];
        } else {
            Arrays.sort(inputPath);
            command = bwa + " mem -t 1 " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa "
                    + " " + inputPath[0] + " " + inputPath[1];
        }
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
            if (!(line.substring(0, 1)).equals("@") || key.toString().equals("1")) {
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
        er.close();
        err.close();
        br.close();
        br_err.close();
        outputStream.close();
        printStream.close();

        //clean working directory
//        Runtime.getRuntime().exec("rm -r " + workingDir.getAbsolutePath() + " " + Conf.PATH_REFERENCE);
        Runtime.getRuntime().exec("rm -r " + workingDir.getAbsolutePath());
//        runCommand("ls -lh " + Conf.PATH_MAIN);

        if (output.length() - "\n".length() > 0) {

              mos.write("genefiles",key.toString(),output.substring(0, output.length() - "\n".length()),"gene/gene-"+key.toString());

            //context.write("", output.substring(0, output.length() - "\n".length()));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    public void runCommand(String command) throws IOException {
        System.out.println("");
        System.out.println("command: " + command);

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
            System.out.println("output: " + line);
        }

        while ((error = br_err.readLine()) != null) {
            //Outputs your process execution
            System.out.println("Terminal: " + error);
        }
        is.close();
        isr.close();
        br.close();
        er.close();
        err.close();
        br_err.close();
    }

    String generateFileName(LongWritable k) {
        return k.toString();
    }
}
