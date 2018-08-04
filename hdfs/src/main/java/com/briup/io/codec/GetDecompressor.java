package com.briup.io.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class GetDecompressor extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GetDecompressor(), args));
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String inPath = conf.get("input");

        String outPath = conf.get("output");
        FileSystem inFs = null, outFs = null;
        inFs = FileSystem.get(URI.create(inPath), conf);
        outFs = FileSystem.getLocal(conf);

        InputStream is = inFs.open(new Path(inPath));
        OutputStream os = outFs.create(new Path(outPath));

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(new Path(inPath));

        CompressionInputStream cis = codec.createInputStream(is);

        IOUtils.copyBytes(cis, os, 4096, true);
        inFs.close();
        outFs.close();
        return 0;
    }
}
