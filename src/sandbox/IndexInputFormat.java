package sandbox;

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

import java.io.IOException;

/**
 * @author yukuwang
 */
public class IndexInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new IndexRecordReader();
    }

    private static class IndexRecordReader extends RecordReader<LongWritable, Text> {

        private final int NLINESTOPROCESS =3;
        private LineReader in;
        private LongWritable key;
        private Text value = new Text();
        private long start = 0;
        private long end = 0;
        private long pos = 0;
        private long keyValue = 1;
        private int maxLineLength = Integer.MAX_VALUE;

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
        public Text getCurrentValue() throws IOException, InterruptedException {
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
            final Path file = split.getPath();

            Configuration conf = context.getConfiguration();
            FileSystem fs = file.getFileSystem(conf);
            start = split.getStart();
            end = start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream filein = fs.open(split.getPath());

            if (start != 0) {
                skipFirstLine = true;
                --start;
                filein.seek(start);
            }
            in = new LineReader(filein, conf);
            if (skipFirstLine) {
                start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
            }
            this.pos = start;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (end - pos <= 0) {
                key = null;
                value = null;
                return false;
            } else {
                if (key == null) {
                    key = new LongWritable();
                }
                key.set(keyValue);

                if (value == null) {
                    value = new Text();
                }

                final Text endLine = new Text("\n");
                int newSize = 0;
                for (int i = 0; i < NLINESTOPROCESS; i ++) {
                    System.out.println("!!!!!!!!!!"+i);
                    Text v = new Text();
                    while (pos < end) {
                        newSize = in.readLine(v, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
                        value.append(v.getBytes(), 0, v.getLength());
                        value.append(endLine.getBytes(), 0, endLine.getLength());
                        if (newSize == 0) {
                            break;
                        }
                        pos += newSize;
                    }
                }
                keyValue ++;
                return true;
            }
        }
    }
}