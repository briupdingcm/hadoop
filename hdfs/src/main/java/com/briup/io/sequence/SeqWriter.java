package com.briup.io.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * hadoop jar target/hdfs-0.0.1.jar com.briup.hdfs.seq.SeqWriter -D
 * output=xxxxxxx
 *
 * @author kevin
 */
public class SeqWriter extends Configured implements Tool {
    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SeqWriter(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path path = new Path(conf.get("output"));

        IntWritable key = new IntWritable();
        Text value = new Text();
        // 获取SequenceFile的写入器对象
        SequenceFile.Writer.Option op1 =
                Writer.file(path);
        SequenceFile.Writer.Option op2 =
                Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option op3 =
                Writer.valueClass(value.getClass());

        SequenceFile.Writer writer =
                SequenceFile.createWriter(
                        conf, op1, op2, op3);

//		SequenceFile.Writer.Option op4 = 
//		Writer.compression(CompressionType.RECORD,
//				new BZip2Codec());
//		SequenceFile.Writer writer = SequenceFile.createWriter(conf, op1, op2, op3, op4);

        for (int i = 0; i < 100; i++) {
            if (i % 3 == 0) writer.sync();
            key.set(i);
            value.set(DATA[i % DATA.length]);
            System.out.printf("[%s]\t%s\t%s\n",
                    writer.getLength(), key, value);
            // 写入数据
            writer.append(key, value);
        }

        writer.close();

        return 0;
    }
}
