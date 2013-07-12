package BWA;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: yukun
 * Date: 12/07/2013
 * Time: 14:01
 */
public class BWAMEMMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("key: " + key);
//            System.out.println("value: " + value);
        context.write(key, value);
    }
}