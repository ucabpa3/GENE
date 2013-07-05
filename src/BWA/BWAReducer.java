/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BWA;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author costas
 */
public class BWAReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
        	
        	 Configuration conf = new Configuration();  
        	    String inputDir = "hdfs://localhost:9000/user/costas/";//设定输入目录 
        	    FileSystem hdfs =FileSystem.get(URI.create(inputDir),conf); //获得HDFS文件系统的对象
        	    Path hdfsFile = new Path("hdfs://localhost:9000/user/costas/output.sai");//设定输出目录  
        	   try{
        		   
        	      FileStatus[] inputFiles = hdfs.listStatus(new Path(inputDir));//FileStatus的listStatus()方法获得一个目录中的文件列表 
        	      FSDataOutputStream out = hdfs.create(hdfsFile);//生成HDFS输出流  
        	      for(int i = 0; i < inputFiles.length; i ++){  
        	            System.out.println(inputFiles[i].getPath().getName());
        	            String extensionName = getExtensionName(inputFiles[i].getPath().getName()).toLowerCase();
        	            System.out.println(extensionName);
        	            if (extensionName.equals("sai")){
        	            FSDataInputStream in = hdfs.open(inputFiles[i].getPath());//打开本地输入流  
        	            byte[] buffer = new byte[256];  
        	            int bytesRead = 0;  
        	            while((bytesRead = in.read(buffer))>0){  
        	            out.write(buffer,0,bytesRead);//通过一个循环来写入  
        	            }
        	            in.close();
        	        }     
        	      }
        	      	out.close();
        	     }catch (IOException e) {  
        	           e.printStackTrace();  
        	     }  	
           // for (IntWritable val : values) {
             //   context.write(key, val);
            //}
        }
        public static String getExtensionName(String filename) {
        	if ((filename != null) && (filename.length() > 0)) {
        	int dot = filename.lastIndexOf('.');
        	if ((dot >-1) && (dot < (filename.length() - 1))) {
        	return filename.substring(dot + 1);
        	}
        	}
        	return filename;
        	} 
    }