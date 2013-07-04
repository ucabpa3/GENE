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
public interface AlgorithmInterface {
    
    public void execute(String fileName, File workingDir, Configuration conf) throws IOException;
    
}
