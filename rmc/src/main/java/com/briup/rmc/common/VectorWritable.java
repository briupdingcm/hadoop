package com.briup.rmc.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

public class VectorWritable implements Writable, Iterable<Preference> {
    private LongWritable id = new LongWritable();
    private Collection<Preference> prefs = new TreeSet<Preference>();

    public static VectorWritable multi(VectorWritable vw, double d) {
        VectorWritable res = new VectorWritable();
        for (Preference pref : vw) {
            res.add(Preference.multi(pref, d));
        }
        return res;
    }

    public void add(Preference pref) {
        prefs.add(pref);
    }

    public void clear() {
        prefs.clear();
    }

    public LongWritable getId() {
        return id;
    }

    public void setId(LongWritable id) {
        this.id.set(id.get());
    }

    @Override
    public String toString() {
        return prefs.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        prefs.clear();
        IntWritable len = new IntWritable();
        len.readFields(in);
        int size = len.get();
        for (int i = 0; i < size; i++) {
            Preference pref = new Preference();
            pref.readFields(in);
            prefs.add(pref);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        IntWritable size = new IntWritable(prefs.size());
        size.write(out);
        for (Preference pref : prefs) {
            pref.write(out);
        }
    }

    @Override
    public Iterator<Preference> iterator() {
        return prefs.iterator();
    }

    public void addAll(VectorWritable value) {
        for (Preference p : value) {
            prefs.add(new Preference(p));
        }
    }

    public void remove(VectorWritable uw) {
        for (Preference p : uw)
            prefs.remove(p);
    }

}
