package genelab;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * @author yukuwang
 */
public class Conf {

    public static String PACKAGE_VERSION = "0.2";
//    public static int N_LINES_PER_CHUNKS = 1400000;
        public static int N_LINES_PER_CHUNKS = 46000;
    public static int MAX_LINE_LENGTH = Integer.MAX_VALUE;
    public static int NUMBER_OF_REDUCERS=12;

    /*LOCAL PATHS*/
    public static String PATH_MAIN = "/home/kpaligia/genelab/";
    public static String PATH_BWA = PATH_MAIN+"bwa";
    public static String PATH_REFERENCE = PATH_MAIN + "reference/";
    public static String PATH_CACHE=PATH_MAIN+"cache/";
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//    public static String PATH_MAIN = "/Users/yukun/genelab/";
//    public static String PATH_BWA = PATH_MAIN;
//    public static String PATH_REFERENCE = PATH_MAIN + "reference/";


    /*HDFS PATHS*/
    public static String HDFS_MAIN="/mapr/mapr-m3-student/myvolume/genelab/";
    public static String HDFS_BWA = HDFS_MAIN+"bwa";
    public static String HDFS_REFERENCE = HDFS_MAIN+"reference/";
    public static String HDFS_INPUT=HDFS_MAIN+"input/";
    public static String HDFS_OUTPUT=HDFS_MAIN+"output/";
    public static String HDFS_TEMP=HDFS_MAIN+"/temp/";
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//    public static String HDFS_BWA = "/user/yukun/bwa";
//    public static String HDFS_REFERENCE = "/user/yukun/reference/";
}
