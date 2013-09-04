/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.mapper;

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

    private static File refIndexRes = new File("/Users/costas/genelab/reference/result/");
    private static String[] exts = {".amb", ".ann", ".bwt", ".pac", ".sa"};
    String mainDir = Conf.PATH_MAIN;
    private String bwa = Conf.PATH_BWA;
    private File refDir = new File(Conf.PATH_REFERENCE);

    @Override
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        System.out.println("key : " + key.toString());
        System.out.println("Value:  " + value.toString());
        Configuration conf = new Configuration();
        FileSystem hdfsFileSystem = FileSystem.get(conf);

        String in = value.toString();
        String[] tokens = in.split("/");
        refDir.mkdir();
        refIndexRes.mkdir();
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

        Process p = Runtime.getRuntime().exec(bwa + " index " + refLocal, new String[]{" "}, refIndexRes);

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

        //compress

//        try {
//            Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
//            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
//            Compressor compressor = null;
//
//
//            byte[] buf = new byte[2048];
//            try {
//                compressor = CodecPool.getCompressor(codec);
//
//                for (String ext : exts) {
//                    File snoop_out = new File(refDir.toString() + "/reference" + ext + ".gz");
//                    snoop_out.createNewFile();
//                    FileOutputStream snoop = new FileOutputStream(snoop_out);
//                    CompressionOutputStream out = codec.createOutputStream(snoop, compressor);
//                    String res = refDir.toString() + "/" + referenceName + ext;
//                    FileInputStream inp = new FileInputStream(res);
//                    //out.flush();
//                    while (true) {
//                        int r = inp.read(buf);
//                        IOUtils.copyBytes(inp, out, 4096, false);
//                        if (r == -1) {
//
//                            out.finish();
//                            inp.close();
//                            break;
//                        }
//
//
//                    }
//
//
//                }
//            } finally {
//                CodecPool.returnCompressor(compressor);
//            }
//        } catch (ClassNotFoundException ex) {
//            Logger.getLogger(BWAIndexMapper.class.getName()).log(Level.SEVERE, null, ex);
//        }

        //copy
        Path refResOnHDFS = new Path("/user/costas/reference");
        for (String ext : exts) {
            Path res = new Path(refDir.toString() + "/reference" + ext);
            System.out.println("Copy : " + res.toString());
            hdfsFileSystem.copyFromLocalFile(res, refResOnHDFS);
        }

    }
}
