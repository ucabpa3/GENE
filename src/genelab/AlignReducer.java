/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package genelab;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author costas
 */
public class AlignReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(key, val);
            }
        }
    }
