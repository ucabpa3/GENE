/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package genelab;

import algorithm.AlgorithmContext;
import algorithm.BWA;
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
public class AlignMapper extends Mapper<Object, Text, Text, IntWritable>{
   
    private AlgorithmContext algContext = new AlgorithmContext();
    
    @Override
    public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
        
        Configuration conf = new Configuration();   
        
        String mainDir = Conf.MAINDIR;
        File workingDir = new File(mainDir+context.getJobID().toString());
        
        new File(mainDir).mkdir();
        workingDir.mkdir();
        
        String inPath = mainDir + "input.txt";
        File input = new File(inPath);
        if(!input.exists()){
            input.createNewFile();
        }
        
       
        String in = value.toString();
        
        FileWriter fw = new FileWriter(input.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
			bw.write(in);
			bw.close();   
              
        algContext.setAlgorithm(new BWA());
        algContext.align(inPath, workingDir,conf);
        //The rest to be copied to the BWA class
                        
        
        }
    }
   
