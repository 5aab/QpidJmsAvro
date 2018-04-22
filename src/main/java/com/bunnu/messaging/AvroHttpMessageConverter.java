package com.bunnu.messaging;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

public class AvroHttpMessageConverter<T extends GenericRecord> extends AbstractHttpMessageConverter<T> {

    @Autowired
    private Schema schema;

    public AvroHttpMessageConverter() {
        super(new MediaType("avro", "binary"));
    }

    @Override
    protected T readInternal(Class<? extends T> arg0, HttpInputMessage arg1)
            throws IOException, HttpMessageNotReadableException {
        System.out.println("in readInternal abbab");
        DatumReader<T> reader = new GenericDatumReader<T>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(arg1.getBody(), null);
        return reader.read(null, decoder);
    }

    @Override
    protected boolean supports(Class<?> arg0) {
        return arg0.isAssignableFrom(Record.class);
    }

    @Override
    protected void writeInternal(T arg0, HttpOutputMessage arg1) throws IOException, HttpMessageNotWritableException {
        System.out.println("in writeInternal abbab");
        Record record = (Record) arg0;
        DatumWriter<T> datumWriter = new GenericDatumWriter<>(record.getSchema());
        Encoder encoder = EncoderFactory.get().binaryEncoder(arg1.getBody(), null);

        datumWriter.write(arg0, encoder);
        encoder.flush();
    }

}
