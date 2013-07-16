package BWA;

import genelab.Conf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.Arrays;

/**
 * User: yukun
 * Date: 15/07/2013
 * Time: 15:04
 */
public class BWAbtReducer extends Reducer<LongWritable, Text, String, String> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        File workingDir = new File(Conf.PATH_MAIN + context.getJobID().toString() + "_" + key);
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

        //aln
        if (inputPath[1] == null || inputPath.equals("")) {
            aln(inputPath[0]);
        } else {
            Arrays.sort(inputPath);
            aln(inputPath[0]);
            aln(inputPath[1]);
        }

        //sampe
        String bwa = Conf.PATH_BWA + "bwa";
        String command;
        if (inputPath[1] == null || inputPath.equals("")) {
            command = bwa + " samse " + Conf.PATH_REFERENCE + "reference.fa " + inputPath[0] + ".sai " + inputPath[0];
        } else {
            command = bwa + " sampe " + Conf.PATH_REFERENCE + "reference.fa " + inputPath[0] + ".sai " +
                    inputPath[1] + ".sai " + inputPath[0] + " " + inputPath[1];
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
        printStream.close();

        //clean working directory
        Runtime.getRuntime().exec("rm -r " + workingDir.getAbsolutePath());

        System.out.println("output: " + output.length());
        if (output.length() - "\n".length() > 0) {
            context.write("", output.substring(0, output.length() - "\n".length()));
        }

    }

    public void aln(String input) throws IOException, InterruptedException {
        String bwa = Conf.PATH_BWA + "bwa";
        String command = bwa + " " + "aln" + " -f" + input + ".sai " + Conf.PATH_REFERENCE + "reference.fa "
                + " " + input;
        System.out.println("command :" + command);
        Process p = Runtime.getRuntime().exec(command);

        InputStream er = p.getErrorStream();
        InputStreamReader err = new InputStreamReader(er);
        BufferedReader br_err = new BufferedReader(err);
        String error;

        while ((error = br_err.readLine()) != null) {
            //Outputs your process execution
            System.out.println("Terminal: " + error);
        }

        OutputStream outputStream = p.getOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        printStream.println();
        printStream.flush();
        printStream.close();
    }
}