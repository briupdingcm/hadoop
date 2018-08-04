package com.briup.io.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * hadoop jar target/hdfs-0.0.1.jar
 * com.briup.hdfs.seq.Put -Dinput=/home/xxxxx
 * -Doutput=./xxxx.seq
 *
 * @author kevin
 */
public class PutSeq extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PutSeq(), args));
    }

    private void construct(File dir, Writer writer) {
        File[] lists = dir.listFiles();
        Stream.of(lists).forEach(f -> {
            try {
                if (f.isDirectory()) {
                    //一个目录的开始，做上同步标记
                    writer.sync();
                    construct(f, writer);
                } else {
                    byte[] content = getData(f);
                    FileKey key = new FileKey();
                    BytesWritable value = new BytesWritable();
                    key.setFileName(f.getPath());
                    key.setLength(f.length());
                    value.set(content, 0, content.length);
                    writer.append(key, value);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 从file文件中读取所有数据，放入字节数组并返回该字节数组
     */
    private byte[] getData(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] content = new byte[1024];
        int length = 0;
        while ((length = fis.read(content, 0, 1024)) > 0) {
            baos.write(content, 0, length);
        }
        fis.close();
        baos.flush();
        byte[] r = baos.toByteArray();
        baos.close();
        return r;
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();

        SequenceFile.Writer.Option op1 = Writer.file(new Path(conf.get("output")));
        SequenceFile.Writer.Option op2 = Writer.keyClass(FileKey.class);
        SequenceFile.Writer.Option op3 = Writer.valueClass(BytesWritable.class);
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, op1, op2, op3);

        File ds = new File(conf.get("input"));
        construct(ds, writer);
        writer.close();
        return 0;
    }

}
