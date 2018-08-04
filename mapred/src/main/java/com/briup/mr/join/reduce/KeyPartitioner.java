package com.briup.mr.join.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<TextTuple, Text> {

    @Override
    public int getPartition(TextTuple key, Text value, int numPartitions) {
        return Math.abs(
                key.getArtistId().hashCode() * 127) % numPartitions;
    }

}
