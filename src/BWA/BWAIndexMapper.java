/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BWA;

import genelab.Conf;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author costas
 */
public class BWAIndexMapper extends Mapper<Object, Text, Text, Text>{
    
    private String bwa = Conf.BWADIR;
    private File refDir = new File(Conf.REFERENCE);
    
    public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
        
        String in = value.toString();
        System.out.println("Value:  "+value.toString());
        
        //Process p = Runtime.getRuntime().exec(bwa +" index ",new String[]{" "} , refDir);
        //int exitValue = p.waitFor();
        
    }
    
}
