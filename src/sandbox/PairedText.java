package sandbox;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yukun
 * Date: 09/07/2013
 * Time: 15:54
 * To change this template use File | Settings | File Templates.
 */
public class PairedText implements WritableComparable {

    private Text input1;
    private Text input2;

    public PairedText(Text input1, Text input2) {
        this.input1 = input1;
        this.input2 = input2;
    }

    public PairedText() {
        input1 = new Text();
        input2 = new Text();
    }

    public Text getInput1() {
        return input1;
    }

    public void setInput1(Text input1) {
        this.input1 = input1;
    }

    public Text getInput2() {
        return input2;
    }

    public void setInput2(Text input2) {
        this.input2 = input2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        new Text(input1).write(dataOutput);
        new Text(input2).write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        input1 = new Text();
        input1.readFields(dataInput);
        input2 = new Text();
        input2.readFields(dataInput);
    }

    @Override
    public int compareTo(Object o) {
        PairedText compared =(PairedText) o;
        return compared.getInput1().compareTo(getInput1())+compared.getInput2().compareTo(getInput2());
    }
}
