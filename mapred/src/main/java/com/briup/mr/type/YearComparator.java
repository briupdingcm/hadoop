package com.briup.mr.type;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearComparator extends WritableComparator {

    public YearComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text y1 = (Text) a;
        Text y2 = (Text) b;

        return y2.compareTo(y1);
    }

}
