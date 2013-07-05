/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BWA;

import genelab.Conf;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author costas
 */
public class BWAMapper extends Mapper<Object, Text, Text, IntWritable>{
   
    static Integer fileID = 1;
    
    public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
        
        Configuration conf = new Configuration();
        LocalFileSystem localFS = new LocalFileSystem(FileSystem.getLocal(conf));   
        FileSystem hdfsFileSystem = FileSystem.get(conf);
        String mainDir = Conf.MAINDIR;
        File workingDir = new File(mainDir+context.getJobID().toString());
        
        new File(mainDir).mkdir();
        System.out.println(localFS.getHomeDirectory());
        workingDir.mkdir();
        
        String inPath = mainDir + "input.fq";
        File input = new File(inPath);
        if(!input.exists()){
            input.createNewFile();
        }
        
       
        String in = value.toString();
        System.out.println("Value:  "+value.toString());
        
        FileWriter fw = new FileWriter(input.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
			bw.write(in);
			bw.close();   
                       
                        
        String executable = Conf.BWADIR;
                
        String result = "";
        
        Path local = new Path(mainDir);
        Path hdfs = new Path(Conf.BWAHDFS);
        String fileName = hdfs.getName();
        File toCopy = new File(executable);

        if (!hdfsFileSystem.exists(hdfs)) {
            System.out.println("File " + fileName + " does not exist on HDFS on location: " + local);
        } else if(!toCopy.exists()){
            hdfsFileSystem.copyToLocalFile(false, hdfs, local);
            System.out.println("File " + fileName + " copied to local machine on location: " + local);
        } 
        
        
            String[] empty = {" "};
            //Process p = Runtime.getRuntime().exec(executable +" " + mainDir+"input.fq", empty , workingDir);
            Process p = Runtime.getRuntime().exec(executable + " "+" aln "+" /Users/costas/genelab/reference.fa " + mainDir+"input.fq > " + fileID.toString()+".sai" , empty , workingDir);
            p.waitFor();
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
            
           /* File Rename = new File(workingDir+"/output.txt");
            if(Rename.exists()){
                File RenamedFile = new File(workingDir+"/"+fileID.toString());
                Rename.renameTo(RenamedFile);
                Path local_output = new Path(workingDir+"/"+fileID.toString());
                
                Path hdfs_output = new Path("/user/costas/output/"+fileID.toString()); 
                hdfsFileSystem.copyFromLocalFile(local_output, hdfs_output);
                
                
            }*/
            fileID++;
        }
    }
   
