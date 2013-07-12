package sandbox;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: yukun
 * Date: 06/07/2013
 * Time: 01:57
 * To change this template use File | Settings | File Templates.
 */
public class test {
    public static String runCommand(String command) throws IOException, InterruptedException {
        String result = "";
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();

        InputStream er = p.getErrorStream();
        InputStreamReader err = new InputStreamReader(er);
        BufferedReader br_err = new BufferedReader(err);
        String error;

        while ((error = br_err.readLine()) != null) {
            result += error + "\n";
        }
        System.out.println(result);
        return result;
    }

    public static void main(String[] args) throws Exception {
        File workingDir = new File(System.getProperty("user.home"));
        System.out.println(workingDir.getAbsolutePath());
        String test = "@SQ\tSN:chr10\tLN:135534747";
        System.out.println(test.substring(0, 1));
// runCommand("/Users/yukun/Desktop/test/bwa sampe /Users/yukun/genelab/reference.fa /Users/yukun/Desktop/test/t1.fq ");
    }
}
