package com.briup.pic;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

public class Observation {
    private String label;
    private int[] pixels;
    private RGB[] rgbs;
    Observation(String label, int[] pixels) {
        this.label = label;
        this.pixels = pixels;
        rgbs = new RGB[pixels.length];
        for (int m = 0; m < pixels.length; m++) {
            int data = pixels[m];
            data = (data >> 10) & 0x00000032;
            RGB color = new RGB();
            color.red = data;//pixels[m] >> 10 & 0x00000032;
            color.green = pixels[m] >> 4 & 0x00000064;
            color.blue = pixels[m] >> 0 & 0x00000032;
            this.rgbs[m] = color;
        }
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int[] getPixels() {
        return pixels;
    }

    public void setPixels(int[] pixels) {
        this.pixels = pixels;
    }

    public RGB[] getRgbs() {
        return rgbs;
    }

    public void save(OutputStream os) {
        PrintStream ps = new PrintStream(os);
        ps.printf("%s", label);
        for (int i = 0; i < pixels.length; i++) {
            ps.printf(",0X%X", pixels[i]);
        }
        ps.println();
    }

    class RGB {
        int red, green, blue;
    }
}
