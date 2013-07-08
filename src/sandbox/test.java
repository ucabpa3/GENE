package sandbox;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created with IntelliJ IDEA.
 * User: yukun
 * Date: 06/07/2013
 * Time: 01:57
 * To change this template use File | Settings | File Templates.
 */
public class test {
    public static void main(String[] args) throws Exception {
//        String temp = "/Users/yukun/genelab/bwa aln /Users/yukun/genelab/reference.fa /Users/yukun/genelab/input.fq > /Users/yukun/genelab/2.sai";
//        System.out.println(temp);
        String[] temp = {"/Users/yukun/genelab/bwa aln /Users/yukun/genelab/reference.fa", ">",
                "/Users/yukun/genelab/2.sai"};
        Process p = Runtime.getRuntime().exec(temp);
        p.waitFor();
        InputStream is = p.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        InputStream er = p.getErrorStream();
        InputStreamReader err = new InputStreamReader(er);
        BufferedReader br_err = new BufferedReader(err);
        String line;
        String error;
        while ((line = br.readLine()) != null) {
            //Outputs your process execution
            System.out.println("Line:" + line);
        }

        while ((error = br_err.readLine()) != null) {
            //Outputs your process execution
            System.out.println(error);
        }
        System.out.println("exit value: " + p.exitValue());
    }
}
