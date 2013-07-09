/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BWA;

import genelab.Conf;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author costas
 */
public class BWAIndexMapper extends Mapper<Object, Text, Text, Text>{
    
    private String bwa = Conf.BWADIR;
    private File refDir = new File(Conf.REFERENCE);
    String mainDir = Conf.MAINDIR;
    
    @Override
    public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
        
        Configuration conf = new Configuration();
        FileSystem hdfsFileSystem = FileSystem.get(conf);
        
        String in = value.toString();
        String[] tokens = in.split("/");
        String refLocal = refDir.toString() + "/" + tokens[tokens.length-1];
        System.out.println("Name:  "+ tokens[tokens.length-1]);
        System.out.println("Path:  " + refLocal);
        
        Path local = new Path(refDir.toString());
        Path hdfs = new Path(in);
        String fileName = hdfs.getName();
 
       // File toCopy = new File(executable);

       /* if (!hdfsFileSystem.exists(hdfs)) {
            System.out.println("File " + fileName + " does not exist on HDFS on location: " + local);
        } else {
            hdfsFileSystem.copyToLocalFile(false, hdfs, local);
            System.out.println("File " + fileName + " copied to local machine on location: " + local);
        } */
        
        //Process p = Runtime.getRuntime().exec(bwa +" index "+refDir.toString()+"/"+,new String[]{" "} , refDir);
        //int exitValue = p.waitFor();
        //copy result to hdfs.
    }
    
}
