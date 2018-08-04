package com.briup.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.stream.Stream;

import static java.lang.System.out;

public class ListBlock extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ListBlock(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        HdfsDataInputStream hdis = (HdfsDataInputStream) fs.open(new Path(conf.get("path")));

        hdis.getAllBlocks().stream().forEach(block -> out(block));
        return 0;
    }

    public void out(LocatedBlock block) {
        System.out.println("--------------------");
        ExtendedBlock eb = block.getBlock();

        System.out.printf("block id: %s\n", eb.getBlockId());
        System.out.printf("block Name: %s\n", eb.getBlockName());
        System.out.printf("block Bytes: %s\n", eb.getNumBytes());
        System.out.println("block start offset: " + block.getStartOffset());
        //输出数据块Id及所在数据节点的ip地址
        Stream.of(block.getLocations()).forEach(
                inf -> out.printf("(%s:%d)", inf.getIpAddr(), inf.getInfoPort()));
        System.out.println();
    }
}