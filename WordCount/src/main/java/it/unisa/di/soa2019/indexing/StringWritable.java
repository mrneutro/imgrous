package it.unisa.di.soa2019.indexing;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringWritable implements WritableComparable<StringWritable> {
    private String key;
    private String val;

    public StringWritable() {
    }

    public StringWritable(String key, String val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return key;
    }

    public String getVal() {
        return val;
    }

    @Override
    public int compareTo(StringWritable o) {
        String s1 = o.getKey() + o.getVal();
        String s2 = this.getKey() + this.getVal();
        return s1.compareTo(s2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(key);
        dataOutput.writeUTF(val);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key = dataInput.readUTF();
        val = dataInput.readUTF();
    }
}
