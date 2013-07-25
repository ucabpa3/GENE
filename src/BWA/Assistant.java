package BWA;

import genelab.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * User: yukun
 * Date: 18/07/2013
 * Time: 12:19
 */
public class Assistant {

    public static void copyReference(Configuration conf) throws IOException {
        String refName = conf.get("reference");
        File refFile = new File(Conf.PATH_REFERENCE + refName);
        System.out.println("refFile: " + refFile);
        if (!refFile.exists()) {
            refFile.mkdir();
            //create working folder
            FileSystem hdfsFileSystem = FileSystem.get(conf);
            Path hdfs = new Path(Conf.HDFS_REFERENCE + refName);
            System.out.println("hadfs: "+hdfs);
            FileStatus[] status = hdfsFileSystem.listStatus(hdfs);
            for (int i = 0; i < status.length; i++) {
                System.out.println(status[i].getPath());
                hdfsFileSystem.copyToLocalFile(false, status[i].getPath(), new Path(refFile.getAbsolutePath() + "/" + status[i].getPath().getName()));
            }
        }


    }

    public static void copyBWA(Configuration conf) throws IOException {
        //create working folder
        File bwaFile = new File(Conf.PATH_BWA + "bwa");
        if (!bwaFile.exists()) {
            FileSystem hdfsFileSystem = FileSystem.get(conf);
            System.out.println("home :" + hdfsFileSystem.getHomeDirectory());
            Path hdfs = new Path(Conf.HDFS_BWA);
            FileStatus[] status = hdfsFileSystem.listStatus(hdfs);
            hdfsFileSystem.copyToLocalFile(false, hdfs, new Path(bwaFile.getAbsolutePath()));
        }

    }

}
