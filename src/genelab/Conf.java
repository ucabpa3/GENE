package genelab;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author yukuwang
 */
public class Conf {
    
    public static int N_LINES_PER_CHUNKS=4;
    public static int MAX_LINE_LENGTH=Integer.MAX_VALUE;
    
    /*LOCAL PATHS*/
    public static String MAINDIR = "genelab";
    public static String BWADIR = MAINDIR + "bwa";
    
    /*HDFS PATHS*/
    public static String BWAHDFS = "/user/costas/bwa";
    
}
