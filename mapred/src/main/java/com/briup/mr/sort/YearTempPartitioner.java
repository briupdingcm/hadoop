package com.briup.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearTempPartitioner extends Partitioner<YearTemp, Text> {

    @Override
    public int getPartition(YearTemp key,
                            Text value, int numPartitions) {
        return Math.abs(key.getYear().hashCode() * 127) % numPartitions;
    }

}
