package com.briup.rmc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SeqReader
        extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SeqReader(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path path = new Path(conf.get("input"));

        SequenceFile.Reader.Option op1 =
                SequenceFile.Reader.file(path);
        SequenceFile.Reader reader =
                new SequenceFile.Reader(conf, op1);

        Writable key = (Writable)
                reader.getKeyClass().newInstance();
        Writable value =
                (Writable) reader.getValueClass().newInstance();
        reader.getCompressionCodec();
        reader.getCompressionType();
        long position = reader.getPosition();
        reader.seek(reader.getPosition());
        while (reader.next(key, value)) {
            String syncSeen = reader.syncSeen() ? "*" : "";
            //if (reader.syncSeen()) {
            System.out.printf("[%s%s]\t%s\t%s\n",
                    position, syncSeen, key, value);
            //}
            position = reader.getPosition();
            reader.seek(reader.getPosition());

        }

        reader.close();

        return 0;
    }
}
