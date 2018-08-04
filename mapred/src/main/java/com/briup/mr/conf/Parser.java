package com.briup.mr.conf;

import org.apache.hadoop.io.Text;

public interface Parser {
    boolean parse(String line);

    boolean parse(Text line);

    int getCount();

    String get(int index);
}
