package com.briup.pic;

public class ManhattnDistance implements IDistance {

    @Override
    public double distance(Observation o1, Observation o2) {
        double dis = 0;
        for (int i = 0; i < o1.getPixels().length; i++) {
            dis += (Math.abs(o1.getRgbs()[i].blue - o2.getRgbs()[i].blue) +
                    Math.abs(o1.getRgbs()[i].red - o2.getRgbs()[i].red) +
                    Math.abs(o1.getRgbs()[i].green - o2.getRgbs()[i].green)) / 3;
        }
        // TODO Auto-generated method stub
        return dis;
    }

}
