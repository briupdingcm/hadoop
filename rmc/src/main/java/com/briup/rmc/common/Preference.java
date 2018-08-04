package com.briup.rmc.common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Preference implements WritableComparable<Preference> {
    private LongWritable id;
    private DoubleWritable value;

    public Preference() {
        id = new LongWritable();
        value = new DoubleWritable();
    }

    public Preference(LongWritable id, DoubleWritable value) {
        this.id = new LongWritable(id.get());
        this.value = new DoubleWritable(value.get());
    }

    public Preference(Preference pref) {
        id = new LongWritable(pref.id.get());
        value = new DoubleWritable(pref.value.get());
    }

    public Preference(long id, double value) {
        this.id = new LongWritable(id);
        this.value = new DoubleWritable(value);
    }

    public static Preference multi(Preference pref, double d) {
        return new Preference(pref.id,
                new DoubleWritable(pref.value.get() * d));
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        id.readFields(in);
        value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        value.write(out);
    }

    @Override
    public String toString() {
        return id + ":" + value;
    }

    public LongWritable getId() {
        return id;
    }

    public void setId(long id) {
        this.id.set(id);
    }

    public DoubleWritable getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value.set(value);
    }

    @Override
    public int hashCode() {
        return Math.abs(id.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Preference)) return false;
        Preference p = (Preference) obj;
        return (id == p.id);
    }

    @Override
    public int compareTo(Preference o) {
        return (id.compareTo(o.id));
    }

    public Preference multi(double d) {
        value.set(value.get() * d);
        return this;
    }

    public void set(long id, double value) {
        this.id.set(id);
        this.value.set(value);
    }

}
