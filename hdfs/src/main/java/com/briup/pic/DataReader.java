package com.briup.pic;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DataReader {
    private static int[] getLine(String line) {
        if (line.startsWith("}"))
            return null;
        String[] eles = line.split(",");
        int[] res = new int[eles.length / 2];
        int size = 0;
        for (int i = 0; i < eles.length; i = i + 2) {
            String s = eles[i + 1].trim().toUpperCase().split("X")[1] + eles[i].trim().toUpperCase().split("X")[1];
            int low = Integer.parseUnsignedInt(eles[i].trim().toUpperCase().split("X")[1], 16);
            int high = Integer.parseUnsignedInt(eles[i + 1].trim().toUpperCase().split("X")[1], 16);
            int data = (high << 8) | ((low) & 0x00000011);
            res[size++] = Integer.parseUnsignedInt(s, 16);

        }
        return res;
    }

    private static Observation observationFactory(InputStream is, String label) throws IOException {
        int data = 0;
        byte ch;
        while ((data = is.read()) != -1) {
            ch = (byte) data;
            if (ch == '{')
                break;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line = br.readLine();
        int[] header = getLine(line);
        int counter = 0;
        int[] content = new int[262144];

        while ((line = br.readLine()) != null) {
            int[] r = getLine(line);
            if (r == null)
                break;
            System.arraycopy(r, 0, content, counter, 8);
            counter += 8;
        }
        return new Observation(label, content);
    }

    public static Observation[] ReadObservations(String dataPath) throws Exception {
        InputStream is;
        List<Observation> ls = new ArrayList<Observation>();
        File fs = new File(dataPath);
        for (File f : fs.listFiles()) {
            is = new FileInputStream(f);
            ls.add(observationFactory(is, getLabel(f)));
        }
        Observation[] obs = new Observation[ls.size()];
        return ls.toArray(obs);
    }

    public static String getLabel(File f) {
        return f.getName().substring(0, 1);
    }
}
