package inputFormat;

import genelab.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import sandbox.FQSplitInfo;

import java.io.IOException;

/**
 * @author yukuwang
 */
public class FQInputFormat extends FileInputFormat<LongWritable, FQSplitInfo> {

    @Override
    public RecordReader<LongWritable, FQSplitInfo> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NLinesRecordReader();
    }

    private static class NLinesRecordReader extends RecordReader<LongWritable, FQSplitInfo> {

        private final int NLINESTOPROCESS = Conf.N_LINES_PER_CHUNKS;
        private LineReader in;
        private LongWritable key;
        private FQSplitInfo value;
        private long start = 0;
        private long end = 0;
        private long pos = 0;
        private long keyValue = 1;
        private int maxLineLength = Conf.MAX_LINE_LENGTH;
        private String path;

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public FQSplitInfo getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float) (end - start));
            }
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) genericSplit;
            final Path splitPath = split.getPath();
            path = splitPath.toString();

            Configuration conf = context.getConfiguration();
            FileSystem fs = splitPath.getFileSystem(conf);
            start = split.getStart();
            end = start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream fsDataInputStream = fs.open(split.getPath());

            if (start != 0) {
                skipFirstLine = true;
                --start;
                fsDataInputStream.seek(start);
            }
            in = new LineReader(fsDataInputStream, conf);
            if (skipFirstLine) {
                start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
            }
            this.pos = start;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(keyValue);
            if (value == null) {
                value = new FQSplitInfo();
            }
            int newSize = 0;
            long length=0;
            long start =pos;
            for(int i=0;i<NLINESTOPROCESS;i++){
                Text v = new Text();
                while (pos < end) {
                    newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
                    if (newSize == 0) {
                        break;
                    }
                    length+=newSize;
                    pos += newSize;
                    if (newSize < maxLineLength) {
                        break;
                    }
                }
            }

            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                keyValue++;
                value= new FQSplitInfo(path,start,length);
                return true;
            }
        }

    }
}
