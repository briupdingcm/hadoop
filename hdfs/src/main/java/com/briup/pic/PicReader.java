package com.briup.pic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.PrintStream;
import java.net.URI;

import static java.lang.System.out;

public class PicReader extends Configured implements Tool {
    FSDataOutputStream os = null;

    public static String getLabel(File f) {
        return f.getName().substring(0, 1);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PicReader(), args));

    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String outPath = conf.get("output");


        FileSystem outFs = null;
        outFs = FileSystem.get(URI.create(outPath), conf);

        os = outFs.create(new Path(outPath), new Progressable() {
            public void progress() {
                out.printf("thread: %s, byte: %d\n", Thread.currentThread().getName(), os.getPos());
            }
        });
        PrintStream ps = new PrintStream(os);
        File fs = new File("/Users/kevin/data/pic/");
        for (File _file : fs.listFiles()) {

            Image src = javax.imageio.ImageIO.read(_file); // 构造Image对象
            int wideth = src.getWidth(null); // 得到源图宽
            int height = src.getHeight(null); // 得到源图长

            BufferedImage tag = new BufferedImage(wideth, height, BufferedImage.TYPE_BYTE_BINARY);
            tag.getGraphics().drawImage(src, 0, 0, wideth, height, null); // 绘制缩小后的图
            for (int i = 1; i < 100; i++) {
                tag = BImage.lowFilter(tag);
            }
            tag = BImage.DFTFilter(tag);
            tag = BImage.cut(tag);
            tag = BImage.fill(tag);
            for (int i = 1; i < 100; i++) {
                tag = BImage.lowFilter(tag);
            }

            tag = BImage.DFTFilter(tag);
            BufferedImage tag1 = new BufferedImage(64, 64, BufferedImage.TYPE_BYTE_BINARY);
            tag1.getGraphics().drawImage(tag, 0, 0, 64, 64, null); // 绘制缩小后的图

            String label = getLabel(_file);
            ps.printf("%s", label);
            for (int i = 0; i < tag1.getWidth(); i++) {
                for (int j = 0; j < tag1.getHeight(); j++) {
                    int res = 0;
                    if (tag1.getRGB(i, j) == 0Xffffffff) res = 1;
                    else res = 0;
                    ps.printf(",%1d", res);
                }
            }
            ps.println();
            //ImageIO.write(tag, "GIF", os /* target */ );

            os.flush();
        }
        outFs.close();
        out.println();
        return 0;
    }
}
