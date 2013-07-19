package inputFormat;

import genelab.Conf;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

public class CombinedInputFormat extends CombineFileInputFormat{

    @Override
    public RecordReader <LongWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException {
        return new NLinesRecordReader();
    }
    
    private static class NLinesRecordReader extends RecordReader<LongWritable, Text> {

        private final int NLINESTOPROCESS = Conf.N_LINES_PER_CHUNKS;
        private LineReader[] in = new LineReader[2];
        private LongWritable key;
        private Text value = new Text();
        private long[] start = {0,0};
        private long[] end = {0,0};
        private long[] pos = {0,0};
        private long keyValue = 1;
        private int maxLineLength = Conf.MAX_LINE_LENGTH;
        private Path[] file = new Path[2];

        @Override
        public void close() throws IOException {
            if (in[0] != null) {
                in[0].close();
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
            if (start[0] == end[0]) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos[0] - start[0]) / (float) (end[0] - start[0]));
            }
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            CombineFileSplit split = (CombineFileSplit) genericSplit;
            file = split.getPaths();
            Configuration conf = context.getConfiguration();
            
            for(int i =0;i<2;i++){

                FileSystem fs = file[i].getFileSystem(conf);
                start[i] = split.getOffset(i);
                end[i] = start[i] + split.getLength(i);
                boolean skipFirstLine = false;
                FSDataInputStream filein = fs.open(split.getPath(i));
                
                if (start[i] != 0) {
                skipFirstLine = true;
                --start[0];
                filein.seek(start[0]);
                }
                
                in[i] = new LineReader(filein, conf);
                if (skipFirstLine) {
                 start[i] += in[i].readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end[i] - start[i]));
                }
                this.pos[i] = start[i];
            }
            
            
            
            

            
            
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(keyValue);
            if (value == null) {
                value = new Text();
            }
            value.clear();
            final Text endline = new Text("\n");
            int newSize = 0;
            for(int j = 0; j<2; j++){
                value.append(file[j].toString().getBytes(), 0, file[j].toString().length());
                value.append(endline.getBytes(), 0, endline.getLength());
            for (int i = 0; i < NLINESTOPROCESS ; i++) {
                Text v = new Text();  
               // System.out.println("pos : " + j);
               // System.out.println("val : " + value.toString());
                while (pos[j] < end[j]) {
                    newSize = in[j].readLine(v, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end[j] - pos[j]), maxLineLength));
                    value.append(v.getBytes(), 0, v.getLength());
                    value.append(endline.getBytes(), 0, endline.getLength());
                    if (newSize == 0) {
                        break;
                    }
                    pos[j] += newSize;
                    if (newSize < maxLineLength) {
                        break;
                    }
                   
                }
                              
            }
            long delim = pos[j];
            
            }
           /* Text v = new Text();
            newSize = in[j].readLine(v, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end[j] - pos[j]), maxLineLength));
            value.append(v.getBytes(), 0, v.getLength());
            System.out.println("curr  : " + value.toString());*/
            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                keyValue++;
                return true;
            }
        
            
            
        }
        
    }
}