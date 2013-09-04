package hadoop.reducer;

import genelab.Assistant;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: yukun
 * Date: 29/08/2013
 * Time: 21:25
 */
public class AlignmentReducer extends Reducer<LongWritable, Text, Text, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        System.out.println(key);
        for(Text t:values){
            System.out.println(t);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
//        Assistant.merge(context.getConfiguration());
//        Assistant.appendRest(context.getConfiguration());
    }
}
