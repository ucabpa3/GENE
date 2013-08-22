package BWA;

import genelab.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;

/**
 * User: yukun
 * Date: 18/07/2013
 * Time: 12:19
 */
public class Assistant {

    public static void copyReference(Configuration conf) throws IOException {
        String refName = conf.get("reference");
        FileSystem fs = FileSystem.get(conf);
        Path hdfs = new Path(Conf.HDFS_REFERENCE + refName);
        FileStatus[] status = fs.listStatus(hdfs);
        for (int i = 0; i < status.length; i++) {
            File temp = new File(Conf.PATH_REFERENCE + refName + "/" + status[i].getPath().getName());
            if (!temp.exists()) {
                fs.copyToLocalFile(false, status[i].getPath(), new Path(temp.getAbsolutePath()));
            }
        }
    }

    public static void copyBWA(Configuration conf) throws IOException {
        File bwaFile = new File(Conf.PATH_BWA);
        if (!bwaFile.exists()) {
            FileSystem fs = FileSystem.get(conf);
            Path hdfs = new Path(Conf.HDFS_BWA);
            FileStatus[] status = fs.listStatus(hdfs);
            fs.copyToLocalFile(false, hdfs, new Path(bwaFile.getAbsolutePath()));
        }
    }

    public static void appendResult(Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            String outputPath = conf.get("outputPath");
            Path pathResult = fs.listStatus(new Path(outputPath + "/result"))[0].getPath();
            Path pathLocked = new Path(outputPath + "/result/locked");
            if (!pathResult.getName().equals("locked")) {
                int currentNum = Integer.valueOf(pathResult.getName());
                fs.rename(pathResult, pathLocked);
                FileStatus[] tempFiles = fs.listStatus(new Path(outputPath + "/temp"));
                int length = tempFiles.length;
                for (int n = 0; n < length; n++) {
                    try {
                        int temN = Integer.valueOf(tempFiles[n].getPath().getName());
                        if (temN == currentNum + 1) {
                            System.out.println("merging " + temN);
                            FSDataInputStream in = fs.open(tempFiles[n].getPath());
                            FSDataOutputStream out = fs.append(pathLocked, 1048576);
                            if (tempFiles[n].getLen() < 10) {
                                log("file " + tempFiles[n].getPath().getName() + " seems wrong, only " + tempFiles[n].getLen() + " bytes big.", conf);
                            }
                            byte buffer[] = new byte[1048576];
                            int bytesRead = 0;
                            while ((bytesRead = in.read(buffer)) > 0) {
                                out.write(buffer, 0, bytesRead);
                            }
                            in.close();
                            out.close();
                            fs.delete(tempFiles[n].getPath(), true);
                            length -= 1;
                            currentNum += 1;
                        }
                    } catch (Exception e) {
                    }
                }
                fs.rename(pathLocked, new Path(outputPath.toString() + "/result/" + currentNum));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void appendRest(Configuration conf) throws IOException {
        System.out.println("appendRest");
        FileSystem fs = FileSystem.get(conf);
        String outputPath = conf.get("outputPath");
        Path pathResult = fs.listStatus(new Path(outputPath + "/result"))[0].getPath();
        while (pathResult.getName().equals("locked")) {

        }
        int currentNum = Integer.valueOf(pathResult.getName());
        while (fs.listStatus(new Path(outputPath + "/temp")).length > 0) {
            FileStatus[] tempFiles = fs.listStatus(new Path(outputPath + "/temp"));
            int length = tempFiles.length;
            for (int n = 0; n < length; n++) {
                    int temN = Integer.valueOf(tempFiles[n].getPath().getName());
                    if (temN == currentNum + 1) {
                        System.out.println("merging " + temN);
                        FSDataInputStream in = fs.open(tempFiles[n].getPath());
                        FSDataOutputStream out = fs.append(pathResult, 1048576);
                        if (tempFiles[n].getLen() < 10) {
                            log("file " + tempFiles[n].getPath().getName() + " seems wrong, only " + tempFiles[n].getLen() + " bytes big.", conf);
                        }
                        byte buffer[] = new byte[1048576];
                        int bytesRead = 0;
                        while ((bytesRead = in.read(buffer)) > 0) {
                            out.write(buffer, 0, bytesRead);
                        }
                        in.close();
                        out.close();
                        fs.delete(tempFiles[n].getPath(), true);
                        length -= 1;
                        currentNum += 1;
                    }
            }
        }
        fs.rename(pathResult, new Path(outputPath + "/result/" + currentNum));
    }

    public static void merge(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path cache = new Path(conf.get("outputPath") + "/temp");
        FileStatus[] status = fs.listStatus(cache);
        try {
            Path outFile = new Path(conf.get("outputPath") + "/result.bam");
            if (fs.exists(outFile)) {
                fs.delete(outFile, true);
            }
            FSDataOutputStream out = fs.create(outFile);
            log("trying to merge " + status.length + " files", conf);
            for (int i = 1; i <= status.length; i++) {
                System.out.println(100 * i / status.length + "% has been processed");
                FileStatus fileStatus = fs.getFileStatus(new Path(cache.toString() + "/" + i));
                if (fileStatus.getLen() < 10) {
                    log("file " + fileStatus.getPath().getName() + " seems wrong, only " + fileStatus.getLen() + " bytes big.", conf);
                }
                FSDataInputStream in = fs.open(new Path(cache.toString() + "/" + i));
                byte buffer[] = new byte[1048576];
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();
            fs.delete(cache, true);
            System.out.println("job successful\n");
        } catch (Exception e) {
            e.printStackTrace();
            log(e.getMessage() + "\n", conf);
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

    public static void log(String info, Configuration conf) throws IOException {
        System.out.println("log: " + info);
        Path output = new Path(conf.get("outputPath") + "/info.txt");
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(output)) {
            fs.createNewFile(output);
        }
        FSDataOutputStream out = fs.append(output);
        out.writeChars(info + "\n");
        out.close();
    }

    public static void log(String info, Mapper.Context context) throws IOException {
        log(context.getTaskAttemptID() + ":\n" + info + "\n", context.getConfiguration());
    }

    public static void log(String info, Reducer.Context context) throws IOException {
        log(context.getTaskAttemptID() + ":\n" + info + "\n", context.getConfiguration());
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
