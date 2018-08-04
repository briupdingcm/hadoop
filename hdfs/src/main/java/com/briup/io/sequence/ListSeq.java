package com.briup.io.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class ListSeq extends Configured implements Tool {
    private SequenceFile.Reader reader;

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ListSeq(), args));

    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();

        Path input = new Path(conf.get("input"));
        SequenceFile.Reader.Option op1 = SequenceFile.Reader.file(input);
        reader = new SequenceFile.Reader(conf, op1);

        Writable key = (Writable) reader.getKeyClass().newInstance();
        Writable value = (Writable) reader.getValueClass().newInstance();

        reader.sync(reader.getPosition());

        while (reader.next(key, value)) {
            FileKey file = (FileKey) key;
            System.out.printf("%s\n", new File(file.getFileName()).getParent());
            reader.sync(reader.getPosition());
        }

        return 0;
    }

}
