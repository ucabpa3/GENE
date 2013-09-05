/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.mapper;

import genelab.Assistant;
import genelab.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author costas
 */
public class BWAIndexMapper extends Mapper<Object, Text, Text, Text> {

    private static String[] exts = {".amb", ".ann", ".bwt", ".pac", ".sa"};
    private String bwa = Conf.PATH_BWA;
    private File refDir = new File(Conf.PATH_CACHE+"/reference/");

    @Override
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        System.out.println("Value:  " + value.toString());
        Configuration conf = context.getConfiguration();
        FileSystem hdfsFileSystem = FileSystem.get(conf);

        File wDir = new File(Conf.PATH_CACHE);
        wDir.mkdirs();

        Assistant.copyBWA(conf);

        String in = value.toString();
        String[] tokens = in.split("/");
        refDir.mkdir();

        String referenceName = tokens[tokens.length - 1];
        String refLocal = refDir.toString() + "/" + referenceName;

        System.out.println("Name:  " + tokens[tokens.length - 1]);
        System.out.println("Path:  " + refLocal);

        Path local = new Path(refDir.toString());
        Path hdfs = new Path(in);
        String fileName = hdfs.getName();

        if (!hdfsFileSystem.exists(hdfs)) {
            System.out.println("File " + fileName + " does not exist on HDFS on location: " + local);
        } else {
            hdfsFileSystem.copyToLocalFile(false, hdfs, local);
            System.out.println("File " + fileName + " copied to local machine on location: " + local);
        }

        Process p = Runtime.getRuntime().exec(bwa + " index " + refLocal, new String[]{" "}, refDir);

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
            System.out.println("Error:" + error);
        }
        int exitValue = p.waitFor();
        System.out.println("BWA indexing output : " + exitValue);


        //copy
        for (String ext : exts) {
            Path res = new Path(refDir.toString() + "/reference.fa" + ext);
            System.out.println("Copy : " + res.toString());
            hdfsFileSystem.copyFromLocalFile(false, true, res, new Path(Conf.HDFS_REFERENCE+"/"+conf.get("input")+"/reference.fa"+ext));
        }

    }
}
