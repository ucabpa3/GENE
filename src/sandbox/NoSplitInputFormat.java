package sandbox;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * User: yukun
 * Date: 25/07/2013
 * Time: 02:15
 */
public class NoSplitInputFormat extends FileInputFormat {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new MyRecordReader();
    }

    private static class MyRecordReader extends LineRecordReader {

        long k;

        public void initialize(InputSplit genericSplit,
                               TaskAttemptContext context) throws IOException {
            super.initialize(genericSplit, context);
            FileSplit split = (FileSplit) genericSplit;
            final Path splitPath = split.getPath();
            k=splitPath.getName().hashCode();
        }


        @Override
        public LongWritable getCurrentKey() {
            return new LongWritable(k);
        }

    }

}
