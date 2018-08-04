package com.briup.mr.join.reduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextPairComparator extends WritableComparator {
    public TextPairComparator() {
        super(TextTuple.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextTuple o1 = (TextTuple) a;
        TextTuple o2 = (TextTuple) b;
        return o1.getArtistId().compareTo(
                o2.getArtistId());
    }
}
