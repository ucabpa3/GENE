package BWA;

import genelab.Conf;
import inputFormat.FQSplitInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Arrays;
import java.util.Date;

import static BWA.Assistant.appendResult;

/**
 * User: yukun
 * Date: 12/07/2013
 * Time: 14:02
 */

public class BWAMEMReducer extends Reducer<LongWritable, FQSplitInfo, String, String> {

    @Override
    public void reduce(LongWritable key, Iterable<FQSplitInfo> value, Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        File workingDir = new File(Conf.PATH_CACHE + context.getJobID().toString() + "_" + key);
        System.out.println(workingDir.getAbsolutePath());
        if (new File(workingDir.getAbsolutePath()).mkdirs()) {
            System.out.println("created the local working directory");
        }
        context.setStatus("copying bwa");
        context.progress();
        Assistant.copyBWA(context.getConfiguration());
        context.setStatus("copying reference");
        context.progress();
        Assistant.copyReference(context.getConfiguration());

        context.setStatus("writing down input files");
        context.progress();
//        write down .fq files
        Date start = new Date();
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
                outputStream.write(bytes, 0, (int) (info.getLength() % 1048576) - 2);
            }
            in.close();
            outputStream.close();
        }
        Date end = new Date();
        long time = (end.getTime() - start.getTime());
//        Assistant.log("writing down to local took: " + time + "ms", context);
        //start to run command
        context.setStatus("running bwa");
        context.progress();
        String bwa = Conf.PATH_BWA;
        String command;
        if (outputPath[1] == null || outputPath.equals("")) {
            command = bwa + " mem " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa "
                    + " " + outputPath[0];
        } else {
            Arrays.sort(outputPath);
            command = bwa + " mem " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa "
                    + " " + outputPath[0] + " " + outputPath[1];
        }
        System.out.println("command :" + command);
        Process p = Runtime.getRuntime().exec(command);

        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader br_err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String line;
        String error;
        FSDataOutputStream out = null;

        String outputfile = FileOutputFormat.getOutputPath(context) + "/temp/" + key;
        fs.createNewFile(new Path(outputfile+"t"));
        out = fs.append(new Path(outputfile + "t"));

        while ((line = br.readLine()) != null) {
            //Outputs your process execution
            if (!(line.substring(0, 1)).equals("@") || key.toString().equals("1")) {
                String temp = "" + line + "\n";
                try {
                    out.write(temp.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        while ((error = br_err.readLine()) != null) {
            //Outputs your process execution
            System.out.println("Terminal: " + error);
        }

        br.close();
        br_err.close();
        out.close();
        fs.rename(new Path(outputfile + "t"), new Path(outputfile));
        //clean working directory
        context.setStatus("cleaning");
        context.progress();
        Assistant.deleteDir(workingDir);

        context.setStatus("appending");
        context.progress();
        appendResult(conf);
        context.setStatus("finish");
        context.progress();
    }
}
