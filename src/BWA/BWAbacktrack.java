package BWA;

import genelab.Conf;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

/**
 * User: yukun
 * Date: 29/08/2013
 * Time: 23:26
 */
public class BWAbacktrack implements AlignmentAlgorithm {

    public static void aln(String input, String refFolder) throws IOException, InterruptedException {
        String bwa = Conf.PATH_BWA;
        String command = bwa + " " + "aln" + " -f " + input + ".sai " + Conf.PATH_REFERENCE + refFolder + "/reference.fa "
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

    /**
     * @param context
     * @param input
     * @param outputFile
     */
    @Override
    public void alignSingle(Mapper.Context context, String input, Path outputFile,long key) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }
        context.setStatus("bwa aln");
        context.progress();
        aln(input, context.getConfiguration().get("reference"));
        context.setStatus("bwa samse|sampe");
        context.progress();
        String bwa = Conf.PATH_BWA;
        String command = bwa + " samse " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa " + input + ".sai " + input;
        try {
            FSDataOutputStream out = fs.create(outputFile, true);
            Process p = Runtime.getRuntime().exec(command);

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader br_err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line;
            String error;

            while ((line = reader.readLine()) != null) {
                if (!(line.substring(0, 1)).equals("@") || outputFile.getName().equals("1")) {
                    String temp = "" + line + "\n";
                    out.write(temp.getBytes());
                }
            }

            while ((error = br_err.readLine()) != null) {
                //Outputs your process execution
                System.out.println("Terminal: " + error);
            }
            br_err.close();

            out.flush();
            reader.close();

            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param context
     * @param input_1
     * @param input_2
     * @param outputFile
     */
    @Override
    public void alignPaired(Mapper.Context context, String input_1, String input_2, Path outputFile, long key) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }
        aln(input_1, context.getConfiguration().get("reference"));
        aln(input_2, context.getConfiguration().get("reference"));
        String bwa = Conf.PATH_BWA;
        String command = bwa + " sampe " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa " + input_1 + ".sai " +
                input_2 + ".sai " + input_1 + " " + input_2;
        System.out.println(command);
        try {
            FSDataOutputStream out = fs.create(outputFile, true);
            Process p = Runtime.getRuntime().exec(command);

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader br_err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line;
            String error;

            while ((line = reader.readLine()) != null) {
                if (!(line.substring(0, 1)).equals("@") || outputFile.getName().equals("1")) {
                    String temp = "" + line + "\n";
                    out.write(temp.getBytes());
                }
            }

            while ((error = br_err.readLine()) != null) {
                //Outputs your process execution
                System.out.println("Terminal: " + error);
            }
            br_err.close();

            out.flush();
            reader.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
