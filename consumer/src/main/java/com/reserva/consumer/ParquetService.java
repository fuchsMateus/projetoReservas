package com.reserva.consumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

@Service
public class ParquetService {

    private GenericRecord convertToAvroRecord(Reserva reserva, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("idReserva", reserva.getIdReserva().toString());
        record.put("nomeHospede", reserva.getNomeHospede());
        record.put("emailHospede", reserva.getEmailHospede());
        record.put("dataCheckIn", (int) reserva.getDataCheckIn().toEpochDay());
        record.put("dataCheckOut", (int) reserva.getDataCheckOut().toEpochDay());
        record.put("statusReserva", reserva.getStatusReserva());
        record.put("precoTotal", reserva.getPrecoTotal());
        record.put("tipoQuarto", reserva.getTipoQuarto());
        record.put("telefoneContato", reserva.getTelefoneContato());
        return record;
    }

    private byte[] writeToParquet(GenericRecord record, Schema schema) throws IOException {
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

    public byte[] getReservaParquet(Reserva reserva) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/reserva.avsc"));
        try {
            GenericRecord record = convertToAvroRecord(reserva, schema);
            return writeToParquet(record, schema);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
