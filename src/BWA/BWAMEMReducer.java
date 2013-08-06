package BWA;

import genelab.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sandbox.FQSplitInfo;

import java.io.*;
import java.util.Arrays;

/**
 * User: yukun
 * Date: 12/07/2013
 * Time: 14:02
 */

public class BWAMEMReducer extends Reducer<LongWritable, FQSplitInfo, String, String> {


    @Override
    public void reduce(LongWritable key, Iterable<FQSplitInfo> value, Context context) throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        context.setStatus("kill me!!!!!!");
        context.progress();
        context.setStatus("99");
        context.progress();
        //FileSystem hdfsFileSystem = FileSystem.get(conf);
        File workingDir = new File(Conf.PATH_MAIN + context.getJobID().toString() + "_" + key);
        System.out.println(workingDir.getAbsolutePath());

        if (new File(workingDir.getAbsolutePath()).mkdirs()) {
            System.out.println("created the local working directory");
        }

        Assistant.copyReference(context.getConfiguration());
        Assistant.copyBWA(context.getConfiguration());

//        this.runCommand("rm -r job_*");

//        write down .fq files
        String outputPath[] = new String[2];
        for (FQSplitInfo info : value) {
            Path inFile = new Path(info.getPath());
            String outFile = workingDir.getAbsolutePath() + "/" + inFile.getName() + "_" + key + ".fq";
            if (outputPath[0] == null) {
                outputPath[0] = outFile;
            } else {
                outputPath[1] = outFile;
            }
            // Read from and write to new file
            FSDataInputStream in = fs.open(inFile);
            in.seek(info.getStart());
            File file = new File(outFile);
            if (!file.exists()) {
                file.createNewFile();
            } else {
                file.delete();
            }
            FileOutputStream outputStream = new FileOutputStream(file);
            byte[] bytes = new byte[1048576];
            for (int n = 0; n < info.getLength() / 1048576; n++) {
                in.read(bytes);
                outputStream.write(bytes, 0, 1048576);
            }
            if (in.read(bytes) != -1) {
                outputStream.write(bytes, 0, (int) info.getLength() % 1048576);
            }
            in.close();
            outputStream.close();
        }

        //start to run command
        String bwa = Conf.PATH_BWA + "bwa";
        String command;
        if (outputPath[1] == null || outputPath.equals("")) {
            command = bwa + " mem " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa "
                    + " " + outputPath[0];
        } else {
            Arrays.sort(outputPath);
            command = bwa + " mem -t 1 " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa "
                    + " " + outputPath[0] + " " + outputPath[1];
        }
        System.out.println("command :" + command);
        Process p = Runtime.getRuntime().exec(command);

        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader br_err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String line;
        String error;
        FSDataOutputStream out = fs.create(new Path(FileOutputFormat.getOutputPath(context) + "/temp/" + key));
        while ((line = br.readLine()) != null) {
            //Outputs your process execution
            if (!(line.substring(0, 1)).equals("@") || key.toString().equals("1")) {
                String temp = "" + line + "\n";
                out.write(temp.getBytes());
            }
        }

        while ((error = br_err.readLine()) != null) {
            //Outputs your process execution
            System.out.println("Terminal: " + error);
        }


        br.close();
        br_err.close();
        out.close();

        //clean working directory
//        Runtime.getRuntime().exec("rm -r " + workingDir.getAbsolutePath() + " " + Conf.PATH_REFERENCE);
        Runtime.getRuntime().exec("rm -r " + workingDir.getAbsolutePath());

//        runCommand("ls -lh " + Conf.PATH_MAIN);


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
}
