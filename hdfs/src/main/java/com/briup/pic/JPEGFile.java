package com.briup.pic;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class JPEGFile {
    public static void main(String[] args) throws IOException {
        File fs = new File("/Users/kevin/data/pic/");
        for (File _file : fs.listFiles()) {

            Image src = javax.imageio.ImageIO.read(_file); // 构造Image对象
            int wideth = src.getWidth(null); // 得到源图宽
            int height = src.getHeight(null); // 得到源图长

            BufferedImage tag = new BufferedImage(wideth, height, BufferedImage.TYPE_BYTE_BINARY);
            tag.getGraphics().drawImage(src, 0, 0, wideth, height, null); // 绘制缩小后的图
/*			
			FileOutputStream out = new FileOutputStream("b_"+ _file.getName());// "newfile.jpg");
			ImageIO.write(tag, "GIF", out);
			
			out.close();
			
			 out = new FileOutputStream("c_"+ _file.getName());// "newfile.jpg");
				tag = BImage.lowFilter(BImage.lowFilter(BImage.lowFilter(tag)));

			ImageIO.write(tag, "GIF", out);
			
			out.close();
			
			 out = new FileOutputStream("f_"+ _file.getName());// "newfile.jpg");
				tag = BImage.DFTFilter(tag);

			ImageIO.write(tag, "GIF", out);
			
			out.close();
			 out = new FileOutputStream("x_"+ _file.getName());// "newfile.jpg");
				tag = BImage.Rotate(tag, 90);

			ImageIO.write(tag, "GIF", out);
			
			out.close();
			
			 out = new FileOutputStream("r_"+ _file.getName());// "newfile.jpg");
				tag = BImage.cut(tag);

			ImageIO.write(tag, "GIF", out);
			
			out.close();			
*/
            FileOutputStream out1 = new FileOutputStream("r_" + _file.getName());// "newfile.jpg");
            //	FileOutputStream out2 = new FileOutputStream("f_"+ _file.getName());// "newfile.jpg");
            for (int i = 1; i < 100; i++) {
                tag = BImage.lowFilter(tag);
            }
            tag = BImage.DFTFilter(tag);
            tag = BImage.cut(tag);
            tag = BImage.fill(tag);
            for (int i = 1; i < 100; i++) {
                tag = BImage.lowFilter(tag);
            }

            //	ImageIO.write(BImage.lowFilter(tag), "GIF", out1 /* target */ );
            ImageIO.write(BImage.cut(BImage.DFTFilter(BImage.lowFilter(BImage.lowFilter(BImage.DFTFilter(BImage.lowFilter(BImage.lowFilter(BImage.lowFilter(tag)))))))), "GIF", out1 /* target */);

            out1.close();
            //	out2.close();
        }
    }
}
