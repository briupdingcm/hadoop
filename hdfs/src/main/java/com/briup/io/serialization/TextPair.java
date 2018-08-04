package com.briup.io.serialization;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.*;

public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    // private int third;
    // private List<Text> list = new ArrayList<Text>();
    public TextPair() {
        first = new Text();
        second = new Text();
    }

    /*
     * 复制构造器一定要将另一个同类型对象的属性做整体复制， 而不能是引用复制。
     */
    public TextPair(TextPair tp) {
        // first = tp.first; //引用复制
        first = new Text(tp.first);// 属性复制
        second = new Text(tp.second);
    }

    public static void main(String[] args) throws IOException {
        TextPair t1 = new TextPair();
        t1.setFirst(new Text("54321"));
        t1.setSecond(new Text("12345"));

        TextPair t2 = new TextPair();
        t2.setFirst(new Text("55321"));
        t2.setSecond(new Text("12345"));

        System.out.println(t1.compareTo(t2));

        TextPair.Comparator comparator = new TextPair.Comparator();
        System.out.println(comparator.compare(t1, t2));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream ps = new DataOutputStream(baos);
        t1.write(ps);
        ps.flush();
        byte[] data = baos.toByteArray();
        System.out.println(data.length);
        for (byte b : data) System.out.println(b);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        // out.writeInt(third);
        /*
         * IntWritable iw = new IntWritable(third); iw.write(out);
         */
        // out.writeInt(list.size());
        // for(Text t : list)t.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        // third = in.readInt();
        /*
         * IntWritable iw = new IntWritable(); iw.readFields(in); third =
         * iw.get();
         */
        // int count = in.readInt();
        // list.clear();
        // for(int i = 0; i < count; i++){
        // Text t = new Text();
        // t.readFields(in);
        // list.add(t);
        // }
    }

    @Override
    public int hashCode() {
        return Math.abs(first.hashCode() * 163 + second.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TextPair))
            return false;
        TextPair other = (TextPair) obj;
        return first.equals(other.first) && second.equals(other.second);
    }

    @Override
    public String toString() {
        return first.toString() + "\t" + second.toString();
    }

    public Text getFirst() {
        return first;
    }

    /**
     * 属性复制而不是引用复制
     */
    public void setFirst(Text first) {
        this.first = new Text(first);
    }

    public Text getSecond() {
        return second;
    }

    /**
     * 属性复制而不是引用复制
     */
    public void setSecond(Text second) {
        this.second = new Text(second);
    }

    @Override
    public int compareTo(TextPair o) {
        return first.compareTo(o.first) != 0 ? first.compareTo(o.first) : second.compareTo(o.second);
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TextPair t1 = (TextPair) a;
            TextPair t2 = (TextPair) b;
            return t1.compareTo(t2);
        }

    }
}
