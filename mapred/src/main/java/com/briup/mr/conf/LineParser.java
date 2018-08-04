package com.briup.mr.conf;

import org.apache.hadoop.io.Text;

public class LineParser implements Parser {
    String[] items;

    @Override
    public boolean parse(String line) {
        items = line.split(",");
        return items != null && items.length >= 2;
    }

    @Override
    public boolean parse(Text line) {
        // TODO Auto-generated method stub
        return parse(line);
    }

    @Override
    public int getCount() {
        // TODO Auto-generated method stub
        return items.length;
    }

    @Override
    public String get(int index) {
        // TODO Auto-generated method stub
        return items[index];
    }

}
