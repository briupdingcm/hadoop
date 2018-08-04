package com.briup.mr.outputFormat.writer;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;

public class TarOutputFormat<K, V>
        extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
            throws IOException {
        Configuration conf = context.getConfiguration();
        String extension = ".tar";

        Path file = getDefaultWorkFile(context, extension);
        FileSystem fs = file.getFileSystem(conf);
        OutputStream fileOut = fs.create(file, false);

        //构建tar格式的写入器
        return new TarOutputWriter<K, V>(fileOut);

    }

    private static class TarOutputWriter<K, V>
            extends RecordWriter<K, V> {
        private TarArchiveOutputStream output;

        public TarOutputWriter(OutputStream os) {
            this.output = new TarArchiveOutputStream(os);
        }

        @Override
        public synchronized void write(K key, V value) throws IOException {
            if (key == null || value == null) {
                return;
            }

            TarArchiveEntry mtd =
                    new TarArchiveEntry(key.toString());
            byte[] b = value.toString().getBytes();
            mtd.setSize(b.length);
            output.putArchiveEntry(mtd);
            output.write(b);

            //IOUtils.copyBytes(
            //new ByteArrayInputStream(b), output, 4096, false);
            output.closeArchiveEntry();

        }

        @Override
        public synchronized void close(TaskAttemptContext context) throws IOException {
            if (output != null) {
                output.flush();
                output.finish();
            }
        }
    }
}
