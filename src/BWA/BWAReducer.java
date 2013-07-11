/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BWA;

import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author HaoChen
 */
public class BWAReducer
            extends Reducer<Text, IntWritable, Text, Text> {

	    private Text Outputbam = new Text();
        public void reduce(Text key, Text values,
                Context context) throws Exception {
/*        	
        	 Configuration conf = new Configuration();  
        	    String inputDir = "hdfs://localhost:9000/user/costas/";//set input dir
        	    FileSystem hdfs =FileSystem.get(URI.create(inputDir),conf); //get HDFS file system object
        	    Path hdfsFile = new Path("hdfs://localhost:9000/user/costas/output.sai");//set the output dir 
        	   try{
        		   
        	      FileStatus[] inputFiles = hdfs.listStatus(new Path(inputDir));//FileStatus - listStatus() get a list of the file in this dir 
        	      FSDataOutputStream out = hdfs.create(hdfsFile);//Generate HDFS stream
        	      for(int i = 0; i < inputFiles.length; i ++){  
        	            System.out.println(inputFiles[i].getPath().getName());
        	            String extensionName = getExtensionName(inputFiles[i].getPath().getName()).toLowerCase();
        	            System.out.println(extensionName);
        	            if (extensionName.equals("sai")){
        	            FSDataInputStream in = hdfs.open(inputFiles[i].getPath());//open local input stream  
        	            byte[] buffer = new byte[256];  
        	            int bytesRead = 0;  
        	            while((bytesRead = in.read(buffer))>0){  
        	            out.write(buffer,0,bytesRead);// write the file by using a loop 
        	            }
        	            in.close();
        	        }     
        	      }
        	      	out.close();
        	     }catch (IOException e) {  
        	           e.printStackTrace();  
        	     }
 */  		
        	String temp = "";
        	if( !key.toString().equals("001.bam"))
        	{
        	Scanner scanner = new Scanner(values.toString());
        	while (scanner.hasNextLine()) {
        	  String line = scanner.nextLine();
        	  if (line.indexOf("@SQ") == -1){
        		  temp = temp + line;
        	  }
        	}
        	}else{
        		temp = values.toString();
        	}
        	Outputbam.set(temp);
            context.write(key, Outputbam);
        }
 /*
        public static String getExtensionName(String filename) {
        	if ((filename != null) && (filename.length() > 0)) {
        	int dot = filename.lastIndexOf('.');
        	if ((dot >-1) && (dot < (filename.length() - 1))) {
        	return filename.substring(dot + 1);
        	}
        	}
        	return filename;
        	} 
  */
        
    }

