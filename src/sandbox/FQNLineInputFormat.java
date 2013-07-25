package sandbox;

import genelab.Conf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: yukun
 * Date: 24/07/2013
 * Time: 23:22
 */
public class FQNLineInputFormat extends NLineInputFormat {
    static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
    private static final Log LOG = LogFactory.getLog(NLineInputFormat.class);
    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        return new FQLineRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext job
    ) throws IOException {
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);

        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        for (FileStatus file : files) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            long length = file.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            if ((length != 0) && isSplitable(job, path)) {
                long blockSize = file.getBlockSize();
                long splitSize = computeSplitSize(blockSize, minSize, maxSize);
                long bytesRemaining = length;
                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
                    splits.add(new FQFileSplit(path, length - bytesRemaining, splitSize,
                            blkLocations[blkIndex].getHosts(),
                            getLineNumber(path, length - bytesRemaining, splitSize, job)));
                    bytesRemaining -= splitSize;
                }

                if (bytesRemaining != 0) {
                    splits.add(new FQFileSplit(path, length - bytesRemaining, bytesRemaining,
                            blkLocations[blkLocations.length - 1].getHosts(),
                            getLineNumber(path, length - bytesRemaining, bytesRemaining, job)));
                }
            } else if (length != 0) {
                splits.add(new FQFileSplit(path, 0, length, blkLocations[0].getHosts(),
                        getLineNumber(path, 0, length, job)));
            } else {
                //Create empty hosts array for zero length files
                splits.add(new FQFileSplit(path, 0, length, new String[0], 0));
            }
        }

        // Save the number of input files in the job-conf
        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

        LOG.debug("Total # of splits: " + splits.size());
        return splits;
    }

    public long getLineNumber(Path path, long start, long length, JobContext job) throws IOException {

        Configuration conf = job.getConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        long end = start + length;
        long pos = start;
        FSDataInputStream fsDataInputStream = fs.open(path);
        fsDataInputStream.seek(start);
        LineReader lineReader = new LineReader(fsDataInputStream, conf);
        long n = 0;
        while (pos < end) {
            n++;
            Text v = new Text();
            long newSize = lineReader.readLine(v, Conf.MAX_LINE_LENGTH, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), Conf.MAX_LINE_LENGTH));
            System.out.println("getLineNumber: " + v);
            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < Conf.MAX_LINE_LENGTH) {
                break;
            }
        }
        return n;
    }

    private static class FQLineRecordReader extends RecordReader<LongWritable, Text> {
        private static final Log LOG = LogFactory.getLog(FQLineRecordReader.class);
        private CompressionCodecFactory compressionCodecs = null;
        private long start;
        private long pos;
        private long end;
        private LineReader in;
        private int maxLineLength;
        private LongWritable key = null;
        private Text value = null;
        private long k;

        public void initialize(InputSplit genericSplit,
                               TaskAttemptContext context) throws IOException {
            FQFileSplit split = (FQFileSplit) genericSplit;
            Configuration job = context.getConfiguration();
            this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                    Integer.MAX_VALUE);
            k=split.getStartLine();
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
            if (key == null) {
                key = new LongWritable();
            }
            key.set(pos);
            key.set(k);
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
                key = null;
                value = null;
                return false;
            } else {
                k++;
                return true;
            }
        }

        @Override
        public LongWritable getCurrentKey() {
            return key;
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
