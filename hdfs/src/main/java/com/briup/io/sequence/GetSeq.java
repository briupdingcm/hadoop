package com.briup.io.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class GetSeq extends Configured implements Tool {
    private SequenceFile.Reader reader;

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GetSeq(), args));
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        Path input = new Path(conf.get("input"));
        File output = new File(conf.get("output"));

        SequenceFile.Reader.Option op1 = SequenceFile.Reader.file(input);
        reader = new SequenceFile.Reader(conf, op1);

        Writable key = (Writable) reader.getKeyClass().newInstance();
        Writable value = (Writable) reader.getValueClass().newInstance();
        while (reader.next(key, value)) {
            String file = ((FileKey) key).getFileName();
            save(new File(output, file), value);
        }
        return 0;
    }

    private void save(File file, Writable value) throws IOException {
        File d = file.getParentFile();
        if (!d.exists()) d.mkdirs();

        BytesWritable bw = (BytesWritable) value;
        byte[] bs = bw.copyBytes();

        FileOutputStream fos = new FileOutputStream(file);
        fos.write(bs, 0, bs.length);
        fos.close();

    }

}
