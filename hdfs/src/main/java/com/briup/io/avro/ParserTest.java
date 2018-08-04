package com.briup.io.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import java.io.IOException;

public class ParserTest {

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(ParserTest.class.getResourceAsStream("StringPair.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", new Utf8("L"));
        datum.put("right", new Utf8("R"));
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(System.out, null);
        writer.write(datum, encoder);
        encoder.flush();

    }

}
