package hadoop.reducer;

import genelab.Assistant;
import org.apache.hadoop.fs.*;
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
    FSDataOutputStream out;
    FileSystem fs;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        fs = FileSystem.get(context.getConfiguration());
        Path outFile = new Path(context.getConfiguration().get("outputPath") + "/result.bam");
        if (fs.exists(outFile)) {
            fs.delete(outFile, true);
        }
        out = fs.create(outFile);

    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        for(Text value: values){
            System.out.println(value.toString());
            FSDataInputStream in = fs.open(new Path(value.toString()));
            byte buffer[] = new byte[4096];
            int bytesRead = 0;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
            in.close();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        out.close();
//        Assistant.merge(context.getConfiguration());
//        Assistant.appendRest(context.getConfiguration());
    }
}
