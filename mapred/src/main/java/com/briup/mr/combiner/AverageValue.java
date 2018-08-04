package com.briup.mr.combiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageValue implements Writable {
    private VIntWritable num;
    private DoubleWritable value;

    public AverageValue() {
        num = new VIntWritable(0);
        value = new DoubleWritable(0.0);
    }

    public AverageValue(int c, double s) {
        num = new VIntWritable(c);
        value = new DoubleWritable(s);
    }

    public AverageValue(AverageValue v) {
        num = new VIntWritable();
        num.set(v.num.get());
        value = new DoubleWritable();
        value.set(v.value.get());
    }

    public double sum() {
        return num.get() * value.get();
    }

    public void readFields(DataInput input)
            throws IOException {
        num.readFields(input);
        value.readFields(input);
    }

    public void write(DataOutput output) throws IOException {
        num.write(output);
        value.write(output);
    }

    public void set(int num, double value) {
        this.num.set(num);
        this.value.set(value);
    }

    public AverageValue add(AverageValue other){
        int n = getNum().get() + other.getNum().get();
        double s = sum() + other.sum();
        return new AverageValue(n, s/n);
    }

    public static AverageValue empty(){
        return new AverageValue(0, 0.0);
    }
    public void set(VIntWritable num, DoubleWritable value) {
        set(num.get(), value.get());
    }

    public VIntWritable getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num.set(num);
    }

    public void setNum(VIntWritable num) {
        this.num.set(num.get());
    }

    public DoubleWritable getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value.set(value);
    }

    public void setValue(DoubleWritable value) {
        this.value.set(value.get());
    }

    public String toString() {
        return num + ", " + value;
    }
}
