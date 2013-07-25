package BWA;

import genelab.Conf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: yukun
 * Date: 12/07/2013
 * Time: 14:01
 */
public class BWAMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    String v;
    long k = 0;

    protected void setup(Mapper.Context context)
            throws IOException,
            InterruptedException {
        v = context.getCurrentKey().toString() + "\n";
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        k++;
        v = v + value + "\n";

        if (k % Conf.N_LINES_PER_CHUNKS == 0) {
            context.write(new LongWritable(k / Conf.N_LINES_PER_CHUNKS), new Text(v.substring(0,v.length()-1)));
            v = context.getCurrentKey().toString() + "\n";
        }

    }

    @Override
    protected void cleanup(Mapper.Context context)
            throws IOException,
            InterruptedException {
        context.write(new LongWritable(k / Conf.N_LINES_PER_CHUNKS + 1), new Text(v.substring(0,v.length()-1)));
    }
}