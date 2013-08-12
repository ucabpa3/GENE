package BWA;

import genelab.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;

/**
 * User: yukun
 * Date: 18/07/2013
 * Time: 12:19
 */
public class Assistant {

    public static void copyReference(Configuration conf) throws IOException {
        String refName = conf.get("reference");
        FileSystem hdfsFileSystem = FileSystem.get(conf);
        Path hdfs = new Path(Conf.HDFS_REFERENCE + refName);
        FileStatus[] status = hdfsFileSystem.listStatus(hdfs);
        for (int i = 0; i < status.length; i++) {
            File temp = new File(Conf.PATH_REFERENCE+refName+"/"+status[i].getPath().getName());
            if(!temp.exists()){
                hdfsFileSystem.copyToLocalFile(false, status[i].getPath(),new Path(temp.getAbsolutePath()));
            }
        }
    }

    public static void copyBWA(Configuration conf) throws IOException {
        File bwaFile = new File(Conf.PATH_BWA);
        if (!bwaFile.exists()) {
            FileSystem hdfsFileSystem = FileSystem.get(conf);
            Path hdfs = new Path(Conf.HDFS_BWA);
            FileStatus[] status = hdfsFileSystem.listStatus(hdfs);
            hdfsFileSystem.copyToLocalFile(false, hdfs, new Path(bwaFile.getAbsolutePath()));
        }
    }

    public static void merge(String output) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfsFileSystem = FileSystem.get(conf);
        Path cache = new Path(output + "/temp/");
        FileStatus[] status = hdfsFileSystem.listStatus(cache);
        try {
            Path outFile = new Path(output + "/result.bam");
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataOutputStream out = fs.create(outFile);
            System.out.println("merging " + status.length + " files");
            for (int i = 1; i <= status.length; i++) {
                FSDataInputStream in = fs.open(new Path(output + "/temp/" + i));
                byte buffer[] = new byte[256];
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();
            hdfsFileSystem.delete(cache, true);
            System.out.println("job successful");
        } catch (Exception e) {
            System.out.println("File not found");
        }

    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }

    public static void runCommand(String command) throws IOException {
        System.out.println("");
        System.out.println("command: " + command);

        Process p = Runtime.getRuntime().exec(command);

        InputStream is = p.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        InputStream er = p.getErrorStream();
        InputStreamReader err = new InputStreamReader(er);
        BufferedReader br_err = new BufferedReader(err);
        String line;
        String error;
        String output = "";
        while ((line = br.readLine()) != null) {
            //Outputs your process execution
            System.out.println("output: " + line);
        }

        while ((error = br_err.readLine()) != null) {
            //Outputs your process execution
            System.out.println("Terminal: " + error);
        }
        is.close();
        isr.close();
        br.close();
        er.close();
        err.close();
        br_err.close();
    }

}
