package BWA;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import inputFormat.FQSplitInfo;

import java.io.IOException;

/**
 * User: yukun
 * Date: 12/07/2013
 * Time: 14:01
 */
public class BWAMapper extends Mapper<LongWritable, FQSplitInfo, LongWritable, FQSplitInfo> {

    public void map(LongWritable key, FQSplitInfo value, Context context) throws IOException, InterruptedException {
        System.out.println(value+" "+key);
        context.write(key, value);
    }

}