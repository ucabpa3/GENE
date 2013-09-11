package BWA;

import genelab.Conf;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * User: yukun
 * Date: 28/08/2013
 * Time: 15:17
 */
public class BWAmem implements AlignmentAlgorithm {

    private Process process;

    @Override
    public void alignSingle(Mapper.Context context, String input, Path outputFile, long key) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }

//        String command = Conf.PATH_BWA + " mem " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa "
//                + " " + input;
//        System.out.println(command);
        try {
            ProcessBuilder pb = new ProcessBuilder("Conf.PATH_BWA", "mem", "Conf.PATH_REFERENCE + context.getConfiguration().get(\"reference\") + \"/reference.fa", input);
//            pb.redirectErrorStream(true);
            FSDataOutputStream out = fs.create(outputFile, true);
            process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;

            while ((line = reader.readLine()) != null) {
                //Outputs your process execution
                if (!(line.substring(0, 1)).equals("@") || outputFile.getName().equals("1")) {
                    String temp = "" + line + "\n";
                    out.write(temp.getBytes());

                }
            }
            out.flush();
            reader.close();

            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void alignPaired(Mapper.Context context, String input_1, String input_2, Path outputFile, long key) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (fs.exists(outputFile)) {
            fs.delete(outputFile, true);
        }
        String command = Conf.PATH_BWA + " mem " + Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa "
                + " " + input_1 + " " + input_2;
        System.out.println(command);

        try {
            ProcessBuilder pb = new ProcessBuilder(Conf.PATH_BWA, "mem", Conf.PATH_REFERENCE + context.getConfiguration().get("reference") + "/reference.fa", input_1, input_2);
//            pb.redirectErrorStream(true);
            FSDataOutputStream out = fs.create(outputFile, true);
            process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            if (key == 1) {
                while ((line = reader.readLine()) != null) {
                    String temp = "" + line + "\n";
                    out.write(temp.getBytes());
                }
            } else {
                while ((line = reader.readLine()) != null) {
                    if (line.length() > 0 && !(line.substring(0, 1)).equals("@")) {
                        String temp = "" + line + "\n";
                        out.write(temp.getBytes());
                    }

                }

            }

            out.flush();
            reader.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
