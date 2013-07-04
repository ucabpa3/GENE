package algorithm;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author costas
 */
public class AlgorithmContext {
    
    private AlgorithmInterface alg = null;
    
    public void align(String fileName,File workingDir, Configuration conf) throws IOException{
        alg.execute(fileName, workingDir, conf);
    }
    
    public void setAlgorithm(AlgorithmInterface alg){
        this.alg = alg;
    }
    
}
