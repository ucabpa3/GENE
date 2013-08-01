package sandbox;

/**
 * User: yukun
 * Date: 26/07/2013
 * Time: 02:46
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * NLineInputFormat which splits N lines of input as one split.
 * <p/>
 * In many "pleasantly" parallel applications, each process/mapper
 * processes the same input file (s), but with computations are
 * controlled by different parameters.(Referred to as "parameter sweeps").
 * One way to achieve this, is to specify a set of parameters
 * (one set per line) as input in a control file
 * (which is the input path to the map-reduce application,
 * where as the input dataset is specified
 * via a config variable in JobConf.).
 * <p/>
 * The NLineInputFormat can be used in such applications, that splits
 * the input file such that by default, one line is fed as
 * a value to one map task, and key is the offset.
 * i.e. (k,v) is (LongWritable, Text).
 * The location hints will span the whole mapred cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FQNLineInputFormat2 extends FileInputFormat<Text, Text> {
    public static final String LINES_PER_MAP =
            "mapreduce.input.lineinputformat.linespermap";

    public static List<FQFileSplit> getSplitsForFile(FileStatus status,
                                                     Configuration conf, int numLinesPerSplit) throws IOException {
        List<FQFileSplit> splits = new ArrayList<FQFileSplit>();
        Path fileName = status.getPath();
        if (status.isDir()) {
            throw new IOException("Not a file: " + fileName);
        }
        FileSystem fs = fileName.getFileSystem(conf);
        LineReader lr = null;
        try {
            FSDataInputStream in = fs.open(fileName);
            lr = new LineReader(in, conf);
            Text line = new Text();
            int numLines = 0;
            long begin = 0;
            long length = 0;
            int num = -1;
            while ((num = lr.readLine(line)) > 0) {
                numLines++;
                length += num;
                if (numLines == numLinesPerSplit) {
                    // NLineInputFormat uses LineRecordReader, which always reads
                    // (and consumes) at least one character out of its upper split
                    // boundary. So to make sure that each mapper gets N lines, we
                    // move back the upper split limits of each split
                    // by one character here.
                    if (begin == 0) {
                        splits.add(new FQFileSplit(fileName, begin, length - 1,
                                new String[]{}, splits.size()));
                    } else {
                        splits.add(new FQFileSplit(fileName, begin - 1, length,
                                new String[]{}, splits.size()));
                    }
                    begin += length;
                    length = 0;
                    numLines = 0;
                }
            }
            if (numLines != 0) {
                splits.add(new FQFileSplit(fileName, begin, length, new String[]{}, splits.size()));
            }
        } finally {
            if (lr != null) {
                lr.close();
            }
        }
        return splits;
    }

    /**
     * Set the number of lines per split
     *
     * @param job      the job to modify
     * @param numLines the number of lines per split
     */
    public static void setNumLinesPerSplit(Job job, int numLines) {
        job.getConfiguration().setInt(LINES_PER_MAP, numLines);
    }

    /**
     * Get the number of lines per split
     *
     * @param job the job
     * @return the number of lines per split
     */
    public static int getNumLinesPerSplit(JobContext job) {
        return job.getConfiguration().getInt(LINES_PER_MAP, 1);
    }

    public RecordReader<Text, Text> createRecordReader(
            InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        context.setStatus(genericSplit.toString());
        return new FQLineRecordReader();
    }

    /**
     * Logically splits the set of input files for the job, splits N lines
     * of the input as one split.
     *
     * @see FileInputFormat#getSplits(JobContext)
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int numLinesPerSplit = getNumLinesPerSplit(job);
        for (FileStatus status : listStatus(job)) {
            System.out.println("status: "+status);
            splits.addAll(getSplitsForFile(status,
                    job.getConfiguration(), numLinesPerSplit));
        }
        return splits;
    }

    private static class FQLineRecordReader extends RecordReader<Text, Text> {
        private static final Log LOG = LogFactory.getLog(FQLineRecordReader.class);
        private CompressionCodecFactory compressionCodecs = null;
        private long start;
        private long pos;
        private long end;
        private LineReader in;
        private int maxLineLength;
        private Text value = null;
        private Text k;

        public void initialize(InputSplit genericSplit,
                               TaskAttemptContext context) throws IOException {
            FQFileSplit split = (FQFileSplit) genericSplit;
            Configuration job = context.getConfiguration();
            this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                    Integer.MAX_VALUE);
//            k = new LongWritable(split.getSplitNum());
            k = new Text(split.getPath().getName()+"_"+Integer.toString((int) (long) split.getSplitNum()));
            start = split.getStart();
            end = start + split.getLength();
            final Path file = split.getPath();
            compressionCodecs = new CompressionCodecFactory(job);
            final CompressionCodec codec = compressionCodecs.getCodec(file);

            // open the file and seek to the start of the split
            FileSystem fs = file.getFileSystem(job);
            FSDataInputStream fileIn = fs.open(split.getPath());
            boolean skipFirstLine = false;
            if (codec != null) {
                in = new LineReader(codec.createInputStream(fileIn), job);
                end = Long.MAX_VALUE;
            } else {
                if (start != 0) {
                    skipFirstLine = true;
                    --start;
                    fileIn.seek(start);
                }
                in = new LineReader(fileIn, job);
            }
            if (skipFirstLine) {  // skip first line and re-establish "start".
                start += in.readLine(new Text(), 0,
                        (int) Math.min((long) Integer.MAX_VALUE, end - start));
            }
            this.pos = start;
        }

        public boolean nextKeyValue() throws IOException {
            if (value == null) {
                value = new Text();
            }
            int newSize = 0;
            while (pos < end) {
                newSize = in.readLine(value, maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                                maxLineLength));
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                if (newSize < maxLineLength) {
                    break;
                }

                // line too long. try again
                LOG.info("Skipped line of size " + newSize + " at pos " +
                        (pos - newSize));
            }
            if (newSize == 0) {
                return false;
            } else {
                return true;
            }
        }

        @Override
        public Text getCurrentKey() {
            return k;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        /**
         * Get the progress within the split
         */
        public float getProgress() {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float) (end - start));
            }
        }

        public synchronized void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }
    }
}
