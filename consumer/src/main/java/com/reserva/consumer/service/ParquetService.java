package com.reserva.consumer.service;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class ParquetService {

    @Getter
    @Setter
    private Schema schema;

    public abstract GenericRecord convertToAvroRecord(Object object);
    public byte[] recordToParquet(GenericRecord record) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        OutputFile outputFile = new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() throws IOException {
                        return byteArrayOutputStream.size();
                    }

                    @Override
                    public void write(int b) throws IOException {
                        byteArrayOutputStream.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        byteArrayOutputStream.write(b, off, len);
                    }

                    @Override
                    public void close() throws IOException {
                        byteArrayOutputStream.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long)ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withDictionaryEncoding(false)
                .build()) {
            writer.write(record);
        }

        return byteArrayOutputStream.toByteArray();
    }

}
