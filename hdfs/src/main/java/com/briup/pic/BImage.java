package com.briup.pic;

import java.awt.*;
import java.awt.image.BufferedImage;

public class BImage {
    public static BufferedImage fill(BufferedImage img) {
        int width = img.getWidth();
        int height = img.getHeight();
        int imageData[] = img.getRGB(0, 0, width, height, null, 0, width);
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                int res = imageData[i * width + j];

                if (res == 0xffffffff) imageData[i * width + j] = 0;
                else break;
            }
            for (int j = width - 1; j > 0; j--) {
                int res = imageData[i * width + j];
                if (res == 0xffffffff) imageData[i * width + j] = 0;
                else break;
            }
        }


        BufferedImage res = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_BINARY);
        res.setRGB(0, 0, width, height, imageData, 0, width);
        res.flush();
        return res;
    }

    public static BufferedImage cut(BufferedImage img) {
        int width = img.getWidth();
        int height = img.getHeight();
        int imageData[] = img.getRGB(0, 0, width, height, null, 0, width);
        int startx = width;
        int endx = 0;
        int starty = height;
        int endy = 0;
        int average = 0;
        for (int i = 0; i < width; i++) {
            average = 0;
            for (int j = 0; j < height; j++) {
                average += imageData[i * width + j];
            }
            average /= height;
            if (average != 0xffffffff) {
                starty = i;
                break;
            }
        }
        for (int i = width - 1; i >= 0; i--) {
            average = 0;
            for (int j = 0; j < height; j++) {
                average += imageData[i * width + j];
            }
            average /= height;
            if (average != 0xffffffff) {
                endy = i;
                break;
            }
        }
        for (int j = 0; j < width; j++) {
            average = 0;
            for (int i = 0; i < height; i++) {
                average += imageData[i * width + j];
            }
            average /= height;
            if (average != 0xffffffff) {
                startx = j;
                break;
            }
        }
        for (int j = width - 1; j >= 0; j--) {
            average = 0;

            for (int i = 0; i < height; i++) {
                average += imageData[i * width + j];

            }
            average /= height;
            if (average != 0xffffffff) {
                endx = j;
                break;
            }
        }
        BufferedImage tag1 = new BufferedImage(endx - startx, endy - starty, BufferedImage.TYPE_BYTE_BINARY);
        //	tag1.getGraphics().drawImage(tag, startx, starty, wideth, height, null); // 绘制缩小后的图

        int m = 0, n = 0;
        for (int i = startx; i < endx; i++) {
            for (int j = starty; j < endy; j++) {
                tag1.setRGB(i - startx, j - starty, img.getRGB(i, j));
            }
        }
        return tag1;
    }

    public static BufferedImage Rotate(BufferedImage src, int angel) {
        int src_width = src.getWidth(null);
        int src_height = src.getHeight(null);
        // calculate the new image size
        Rectangle rect_des = CalcRotatedSize(new Rectangle(new Dimension(
                src_width, src_height)), angel);

        BufferedImage res = null;
        res = new BufferedImage(rect_des.width, rect_des.height,
                BufferedImage.TYPE_INT_RGB);
        Graphics2D g2 = res.createGraphics();
        // transform
        g2.translate((rect_des.width - src_width) / 2,
                (rect_des.height - src_height) / 2);
        g2.rotate(Math.toRadians(angel), src_width / 2, src_height / 2);

        g2.drawImage(src, null, null);
        return res;
    }

    public static Rectangle CalcRotatedSize(Rectangle src, int angel) {
        // if angel is greater than 90 degree, we need to do some conversion
        if (angel >= 90) {
            if (angel / 90 % 2 == 1) {
                int temp = src.height;
                src.height = src.width;
                src.width = temp;
            }
            angel = angel % 90;
        }

        double r = Math.sqrt(src.height * src.height + src.width * src.width) / 2;
        double len = 2 * Math.sin(Math.toRadians(angel) / 2) * r;
        double angel_alpha = (Math.PI - Math.toRadians(angel)) / 2;
        double angel_dalta_width = Math.atan((double) src.height / src.width);
        double angel_dalta_height = Math.atan((double) src.width / src.height);

        int len_dalta_width = (int) (len * Math.cos(Math.PI - angel_alpha
                - angel_dalta_width));
        int len_dalta_height = (int) (len * Math.cos(Math.PI - angel_alpha
                - angel_dalta_height));
        int des_width = src.width + len_dalta_width * 2;
        int des_height = src.height + len_dalta_height * 2;
        return new java.awt.Rectangle(new Dimension(des_width, des_height));
    }

    public static BufferedImage lowFilter(BufferedImage img) {
        int width = img.getWidth();
        int height = img.getHeight();
        int newImageData[] = new int[width * height];
        int imageData[] = img.getRGB(0, 0, width, height, null, 0, width);

        int rData[] = new int[width * height];
        int gData[] = new int[width * height];
        int bData[] = new int[width * height];

        for (int i = 0; i < imageData.length; i++) {
            rData[i] = (imageData[i] & 0xff0000) >> 16;
            gData[i] = (imageData[i] & 0xff00) >> 8;
            bData[i] = (imageData[i] & 0xff);
        }

        // 进行3*3矩阵的滤波 ，每个数字是1/9
        for (int v = 1; v <= height - 2; v++) {
            for (int u = 1; u <= width - 2; u++) {
                int sum1 = 0;
                int sum2 = 0;
                int sum3 = 0;
                int pr = 0;
                int pg = 0;
                int pb = 0;
                for (int j = -1; j <= 1; j++) {
                    for (int i = -1; i <= 1; i++) {
                        pr = rData[(v + j) * width + (u + i)];
                        pg = gData[(v + j) * width + (u + i)];
                        pb = bData[(v + j) * width + (u + i)];
                        sum1 = sum1 + pr;
                        sum2 = sum2 + pg;
                        sum3 = sum3 + pb;
                    }
                }
                int q1 = (int) (sum1 / 9.0);
                int q2 = (int) (sum2 / 9.0);
                int q3 = (int) (sum3 / 9.0);

                newImageData[v * width + u] = (255 << 24) | (q1 << 16) | (q2 << 8) | q3;// 新图

            }
        }

        BufferedImage res = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_BINARY);
        res.setRGB(0, 0, width, height, imageData, 0, width);
        res.flush();
        return res;
    }

    public static BufferedImage DFTFilter(BufferedImage img) {
        int w = img.getWidth(null);
        int h = img.getHeight(null);
        int m = get2PowerEdge(w); // 获得2的整数次幂
        // System.out.println(m);
        int n = get2PowerEdge(h);
        // System.out.println(n);
        int[][] last = new int[m][n];
        Complex[][] next = new Complex[m][n];
        int pixel, alpha = -1, newred, newgreen, newblue, newrgb;

        BufferedImage destimg = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_BINARY);
        // ----------------------------------------------------------------------
        // first: Image Padding and move it to center 填充图像至2的整数次幂并乘以（-1）^(x+y)
        // use 2-D array last to store
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                if (i < w && j < h) {
                    pixel = img.getRGB(i, j);
                    if ((i + j) % 2 == 0) {
                        newred = pixel & 0x00ff0000 >> 16;
                    } else {
                        newred = -(pixel & 0x00ff0000 >> 16);
                    }
                    last[i][j] = newred;
                } else {
                    last[i][j] = 0;
                }
            }
        }

        // ----------------------------------------------------------------------
        // second: Fourier Transform 离散傅里叶变换
        // u-width v-height x-width y-height

        // ------------Normal-DFT-------------
        // for (int u = 0; u < m; u++){
        // for (int v = 0; v <n; v++) {
        // next[u][v] = DFT(last, u, v);
        // System.out.println("U: "+u+"---v: "+v);
        // }
        // }
        // if (true) { // 生成DFT图片,记得修改图片大小
        // destimg = showFourierImage(next);
        // return destimg;
        // }

        // ---------------FFT-----------------
        // 先把所有的行都做一维傅里叶变换，再放回去
        Complex[] temp1 = new Complex[n];
        for (int x = 0; x < m; x++) {
            for (int y = 0; y < n; y++) {
                Complex c = new Complex(last[x][y], 0);
                temp1[y] = c;
            }
            next[x] = fft(temp1);
        }

        // 再把所有的列（已经被行的一维傅里叶变换所替代）都做一维傅里叶变换
        Complex[] temp2 = new Complex[m];
        for (int y = 0; y < n; y++) {
            for (int x = 0; x < m; x++) {
                Complex c = next[x][y];
                temp2[x] = c;
            }
            temp2 = fft(temp2);
            for (int i = 0; i < m; i++) {
                next[i][y] = temp2[i];
            }
        }

        // if (true) { // 生成DFT图片,记得修改图片大小
        // destimg = showFourierImage(next);
        // return destimg;
        // }

        // ----------------------------------------------------------------------
        // third: Generate the frequency filter and filter the image in
        // frequency domain 生成频率域滤波器并滤波

        // 构造原始滤波函数
        Complex[][] filter = new Complex[m][n];
        // 这个是11X11均值滤波
        // for (int x = 0; x < m; x++) {
        // for (int y = 0; y < n; y++) {
        // if (x < 11 && y < 11) {
        // if ((x+y)%2==0)
        // filter[x][y] = new Complex(1/121d, 0); // double 后面赋值数字记得加d！！！！！！！
        // else
        // filter[x][y] = new Complex(-1/121d, 0);
        // }
        // else {
        // filter[x][y] = new Complex(0, 0);
        // }
        // }
        // }

        // 下面这个是拉普拉斯滤波
        filter[0][0] = new Complex(0, 0);
        filter[0][1] = new Complex(-1, 0);
        filter[0][2] = new Complex(0, 0);
        filter[1][0] = new Complex(-1, 0);
        filter[1][1] = new Complex(4, 0);
        filter[1][2] = new Complex(-1, 0);
        filter[2][0] = new Complex(0, 0);
        filter[2][1] = new Complex(-1, 0);
        filter[2][2] = new Complex(0, 0);
        for (int x = 0; x < m; x++) {
            for (int y = 0; y < n; y++) {
                if (x < 3 && y < 3) {
                    /* 上面已经写好了 */
                } else {
                    filter[x][y] = new Complex(0, 0);
                }
            }
        }

        // 傅里叶变换 转换为频率域
        for (int x = 0; x < m; x++) {
            for (int y = 0; y < n; y++) {
                Complex c = new Complex(filter[x][y].getR(), filter[x][y].getI());
                temp1[y] = c;
            }
            filter[x] = fft(temp1);
        }

        for (int y = 0; y < n; y++) {
            for (int x = 0; x < m; x++) {
                Complex c = new Complex(filter[x][y].getR(), filter[x][y].getI());
                // Complex c = filter[x][y];
                temp2[x] = c;
            }
            temp2 = fft(temp2);
            for (int i = 0; i < m; i++) {
                filter[i][y] = temp2[i];
            }
        }

        // if (true) {
        // destimg = showFourierImage(filter);
        // return destimg;
        // }

        // point-wise multiply
        Complex[][] g = new Complex[m][n];
        for (int x = 0; x < m; x++) {
            for (int y = 0; y < n; y++) {
                g[x][y] = filter[x][y].times(next[x][y]);
                // System.out.println("g: "+g[x][y].getR()+" "+g[x][y].getI());
            }
        }
        // ----------------------------------------------------------------------
        // fourth: use IDFT to get the image 傅里叶逆变换
        for (int x = 0; x < m; x++) {
            for (int y = 0; y < n; y++) {
                Complex c = new Complex(g[x][y].getR(), g[x][y].getI());
                temp1[y] = c;
            }
            g[x] = ifft(temp1);
        }

        // for (int x = 0; x < m; x++) {
        // for (int y = 0; y < n; y++) {
        // System.out.println("gifft-g: "+g[x][y].getR()+" "+g[x][y].getI());
        // }
        // }

        for (int y = 0; y < n; y++) {
            for (int x = 0; x < m; x++) {
                Complex c = g[x][y];
                temp2[x] = c;
            }
            temp2 = ifft(temp2);
            for (int i = 0; i < m; i++) {
                g[i][y] = temp2[i];
            }
        }

        for (int x = 0; x < m; x++) {
            for (int y = 0; y < n; y++) {
                // System.out.println("ifft-g: "+g[x][y].getR()+"
                // "+g[x][y].getI());
            }
        }
        // ----------------------------------------------------------------------
        // fifth：取实部
        for (int x = 0; x < m; x++) {
            for (int y = 0; y < n; y++) {
                last[x][y] = (int) g[x][y].getR();
                // System.out.println(last[x][y]);
            }
        }
        // ----------------------------------------------------------------------
        // sixth: move the image back and cut the image 乘以(-1)^(x+y)再剪裁图像
        // int srcpixel, srcred;
        int newalpha = (-1) << 24;
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                // srcpixel = img.getRGB(i, j);
                // srcred = srcpixel&0x00ff0000>>16;
                newred = last[i][j];
                if ((i + j) % 2 != 0)
                    newred = -newred;
                // newred = srcred-newred;
                newblue = newred; // 先写这个 ，如果先改变newred的值，newblue也会变成改过后的newred！
                newgreen = newred << 8; // 这个也一样，反正不能放到newred改变自己之前！
                newred = newred << 16;
                newrgb = newalpha | newred | newgreen | newblue;
                destimg.setRGB(i, j, newrgb);
                // System.out.println("R: "+newred+"---G: "+newgreen+"---B:
                // "+newblue);
            }
        }
        // ----------------------------------------------------------------------
        return destimg;

    }

    // 根据图像的长获得2的整数次幂
    public static int get2PowerEdge(int e) {
        if (e == 1)
            return 1;
        int cur = 1;
        while (true) {
            if (e > cur && e <= 2 * cur)
                return 2 * cur;
            else
                cur *= 2;
        }
    }

    // 返回傅里叶频谱图
    public static BufferedImage showFourierImage(Complex[][] f) {
        int w = f.length;
        int h = f[0].length;
        double max = 0;
        double min = 0;
        BufferedImage destimg = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_BINARY);
        // -------------------First get abs(取模)--------------------------
        double[][] abs = new double[w][h];
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                abs[i][j] = f[i][j].abs();
                // System.out.println(f[i][j].getR()+" "+f[i][j].getI());
            }
        }
        // -------------------Second get log(取log + 1)-------------------
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                abs[i][j] = Math.log(abs[i][j] + 1);
            }
        }
        // -------------------Third quantization(量化)---------------------
        max = abs[0][0];
        min = abs[0][0];
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                if (abs[i][j] > max)
                    max = abs[i][j];
                if (abs[i][j] < min)
                    min = abs[i][j];
            }
        }
        int level = 255;
        double interval = (max - min) / level;
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                for (int k = 0; k <= level; k++) {
                    if (abs[i][j] >= k * interval && abs[i][j] < (k + 1) * interval) {
                        abs[i][j] = (k * interval / (max - min)) * level;
                        break;
                    }
                }
            }
        }
        // -------------------Fourth setImage----------------------------
        int newalpha = (-1) << 24;
        int newred;
        int newblue;
        int newgreen;
        int newrgb;
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                newred = (int) abs[i][j] << 16;
                newgreen = (int) abs[i][j] << 8;
                newblue = (int) abs[i][j];
                newrgb = newalpha | newred | newgreen | newblue;
                destimg.setRGB(i, j, newrgb);
            }
        }
        return destimg;
    }

    // normal 2-D DFT
    public static Complex DFT(int[][] f, int u, int v) {
        int M = f.length;
        int N = f[0].length;
        Complex c = new Complex(0, 0);
        for (int x = 0; x < M; x++) {
            for (int y = 0; y < N; y++) {
                Complex temp = new Complex(0, -2 * Math.PI * (u * x / M + v * y / N));
                c = c.plus(temp.exp().times(f[x][y]));
            }
        }
        return c;
    }

    // 快速一维傅里叶变换
    public static Complex[] fft(Complex[] x) { // 传入的全都是206
        int N = x.length;
        // if (N == 256) {
        // for (int i = 0; i < N; i++)
        // System.out.println(i+"---"+x[i].getR()+" "+x[i].getI());
        // }
        // System.out.println(N);

        // base case
        if (N == 1) {
            // System.out.println(x[0].getR()+" "+x[0].getI()); // !!!!ERROR
            // return new Complex[] {x[0]};
            return x;
        }

        // radix 2 Cooley-Turkey FFT
        if (N % 2 != 0) {
            throw new RuntimeException("N is not a power of 2");
        }

        // fft of even terms
        Complex[] even = new Complex[N / 2];
        for (int k = 0; k < N / 2; k++) {
            even[k] = x[2 * k];
        }
        Complex[] q = fft(even);

        // fft of odd terms
        Complex[] odd = new Complex[N / 2];
        for (int k = 0; k < N / 2; k++) {
            odd[k] = x[2 * k + 1]; // DEBUG 之前这里忘记+1 差点搞死我
        }
        Complex[] r = fft(odd);

        // combine
        Complex[] y = new Complex[N];
        for (int k = 0; k < N / 2; k++) {
            double kth = -2 * k * Math.PI / N;
            Complex wk = new Complex(Math.cos(kth), Math.sin(kth)); // all small
            // number
            // not 0
            y[k] = q[k].plus(wk.times(r[k]));
            y[k + N / 2] = q[k].minus(wk.times(r[k]));
            // System.out.println("wk: "+N+"---"+wk.getR()+" "+wk.getI());
            // System.out.println("q[k]: "+N+"---"+q[k].getR()+" "+q[k].getI());
            // System.out.println("r[k]: "+N+"---"+r[k].getR()+" "+r[k].getI());
            // System.out.println("wk.times(r[k]):
            // "+N+"---"+wk.times(r[k]).getR()+" "+wk.times(r[k]).getI());
        }

        return y;
    }

    // 快速一维傅里叶逆变换
    public static Complex[] ifft(Complex[] x) {
        int N = x.length;
        Complex[] y = new Complex[N];

        // take conjugate
        for (int i = 0; i < N; i++) {
            y[i] = x[i].conjugate();
        }

        // compute forward fft
        y = fft(y);

        // take conguate again
        for (int i = 0; i < N; i++) {
            y[i] = y[i].conjugate();
        }

        // divide by N
        for (int i = 0; i < N; i++) {
            y[i] = y[i].times(1.0 / N);
        }

        return y;
    }

    // 快速一维卷积
    public Complex[] convolve(Complex[] x, Complex[] y) {

        if (x.length != y.length) {
            throw new RuntimeException("Dimension don't agree");
        }

        int N = x.length;

        // compute fft of each sequence;
        Complex[] a = fft(x);
        Complex[] b = fft(y);

        // point-wise multiply
        Complex[] c = new Complex[N];
        for (int i = 0; i < N; i++) {
            c[i] = a[i].times(b[i]);
        }

        // compute inverse FFT
        // return ifft(c);
        return c;
    }

    static class Complex {
        private final double r;
        private final double i;

        public Complex(double r, double i) {
            this.r = r;
            this.i = i;
        }

        public double abs() { // return sqrt(r^2 +i^2)
            return Math.hypot(r, i);
        }

        public double phase() {
            return Math.atan2(i, r);
        }

        public Complex plus(Complex c) {
            return new Complex(this.r + c.r, this.i + c.i);
        }

        public Complex minus(Complex c) {
            return new Complex(this.r - c.r, this.i - c.i);
        }

        public Complex times(Complex c) {
            return new Complex(this.r * c.r - this.i * c.i, this.r * c.i + this.i * c.r);
        }

        public Complex times(double d) {
            return new Complex(this.r * d, this.i * d);
        }

        public Complex conjugate() {
            return new Complex(r, -i);
        }

        public double getR() {
            return r;
        }

        public double getI() {
            return i;
        }

        public Complex exp() {
            return new Complex(Math.exp(r) * Math.cos(i), Math.exp(r) * Math.sin(i));
        }
    }
}
