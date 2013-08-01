package sandbox;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: yukun
 * Date: 01/08/2013
 * Time: 00:18
 */
public class FQSplitInfo implements Writable {
    private String path;
    private long start;
    private long length;

    public FQSplitInfo(String path, long start, long length) {
        this.path = path;
        this.start = start;
        this.length = length;
    }

    public FQSplitInfo(){
        path="";
        start =0;
        length=0;
    }

    @Override
    public String toString(){
        return path+" "+start+" "+length;
    }

    public String getPath() {
        return path;
    }

    public long getLength() {
        return length;
    }

    public long getStart() {
        return start;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(path);
        dataOutput.writeLong(start);
        dataOutput.writeLong(length);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       path= dataInput.readUTF();
       start= dataInput.readLong();
        length=dataInput.readLong();
    }
}
