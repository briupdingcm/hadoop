package com.briup.io.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.InputStream;
import java.net.URI;

import static java.lang.System.out;

public class PutCompressor extends Configured implements Tool {
    FSDataOutputStream os = null;

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PutCompressor(), args));

    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inPath = conf.get("input");
        String outPath = conf.get("output");
        FileSystem inFs = null, outFs = null;
        inFs = FileSystem.getLocal(conf);
        outFs = FileSystem.get(URI.create(inPath), conf);

        InputStream is = inFs.open(new Path(inPath));
        os = outFs.create(new Path(outPath), new Progressable() {
            public void progress() {
                out.printf("byte: %d\n", os.getPos());
            }
        });

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(new Path(outPath));
        /*
         * GzipCodec codec = new GzipCodec(); codec.setConf(conf);
         */
        System.out.println(codec.getClass().getName());

        CompressionOutputStream cos = codec.createOutputStream(os);
        IOUtils.copyBytes(is, cos, 4096, true);
        inFs.close();
        outFs.close();
        out.println();

        return 0;
    }

}
