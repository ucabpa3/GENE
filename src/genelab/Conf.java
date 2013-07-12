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
    
    public static int N_LINES_PER_CHUNKS=40;
    public static int MAX_LINE_LENGTH=Integer.MAX_VALUE;
    
    /*LOCAL PATHS*/
    public static String PATH_MAIN = "/Users/yukun/"+"genelab/";
    public static String PATH_BWA = PATH_MAIN;
    public static String PATH_REFERENCE = PATH_MAIN + "reference/";
    
    /*HDFS PATHS*/
    public static String BWAHDFS = "/user/costas/bwa";
    
}
