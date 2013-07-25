package sandbox;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * User: yukun
 * Date: 24/07/2013
 * Time: 23:53
 */
public class FQFileSplit extends InputSplit implements Writable {
    private Path file;
    private long start;
    private long length;
    private long startLine;
    private String[] hosts;



    /** Constructs a split with host information
     *
     * @param file the file name
     * @param start the position of the first byte in the file to process
     * @param length the number of bytes in the file to process
     * @param hosts the list of hosts containing the block, possibly null
     */
    public FQFileSplit(Path file, long start, long length, String[] hosts,long startLine) {
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
        this.startLine=startLine;
    }

    public long getStartLine(){
        return startLine;
    }

    /** The file containing this split's data. */
    public Path getPath() { return file; }

    /** The position of the first byte in the file to process. */
    public long getStart() { return start; }

    /** The number of bytes in the file to process. */
    @Override
    public long getLength() { return length; }

    @Override
    public String toString() { return file + ":" + start + "+" + length; }

    ////////////////////////////////////////////
    // Writable methods
    ////////////////////////////////////////////

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file.toString());
        out.writeLong(start);
        out.writeLong(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file = new Path(Text.readString(in));
        start = in.readLong();
        length = in.readLong();
        hosts = null;
    }

    @Override
    public String[] getLocations() throws IOException {
        if (this.hosts == null) {
            return new String[]{};
        } else {
            return this.hosts;
        }
    }
}