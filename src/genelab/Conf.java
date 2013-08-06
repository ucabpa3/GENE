package genelab;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * @author yukuwang
 */
public class Conf {

    public static String PACKAGE_VERSION = "0.1";
    public static int N_LINES_PER_CHUNKS = 1000000;
    public static int MAX_LINE_LENGTH = Integer.MAX_VALUE;
    /*LOCAL PATHS*/
    public static String PATH_MAIN = "/home/kpaligia/genelab";
    public static String PATH_BWA = PATH_MAIN;
    public static String PATH_REFERENCE = PATH_MAIN + "reference/";
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//    public static String PATH_MAIN = "/Users/yukun/genelab/";
//    public static String PATH_BWA = PATH_MAIN;
//    public static String PATH_REFERENCE = PATH_MAIN + "reference/";


    /*HDFS PATHS*/
    public static String HDFS_MAIN="/mapr/mapr-m3-student/myvolume/genelab/";
    public static String HDFS_BWA = HDFS_MAIN+"bwa";
    public static String HDFS_REFERENCE = HDFS_MAIN+"reference/";
    public static String HDFS_TEMP=HDFS_MAIN+"/temp/";
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//    public static String HDFS_BWA = "/user/yukun/bwa";
//    public static String HDFS_REFERENCE = "/user/yukun/reference/";
}
