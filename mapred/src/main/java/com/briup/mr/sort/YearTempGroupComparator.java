package com.briup.mr.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearTempGroupComparator extends WritableComparator {
    public YearTempGroupComparator() {
        super(YearTemp.class, true);
    }

    @Override
    public int compare(WritableComparable a,
                       WritableComparable b) {
        YearTemp u1 = (YearTemp) a;
        YearTemp u2 = (YearTemp) b;
        return u1.getYear().compareTo(u2.getYear());
    }
}
