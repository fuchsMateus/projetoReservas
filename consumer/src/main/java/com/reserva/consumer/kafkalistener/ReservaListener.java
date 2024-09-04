package com.reserva.consumer.kafkalistener;

import com.reserva.consumer.service.ParquetService;
import com.reserva.consumer.service.S3UploadService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


@Component
public class ReservaListener {

    private final ParquetService parquetService;
    private final S3UploadService s3UploadService;

    //Path para o schema avro da reserva
    private final String SCHEMA_PATH = "src/main/resources/avro/reserva.avsc";
    private final Schema schema;

    public ReservaListener(ParquetService parquetService, S3UploadService s3UploadService) {
        this.parquetService = parquetService;
        this.s3UploadService = s3UploadService;
        try {
            this.schema = new Schema.Parser().parse(new File(SCHEMA_PATH));
        } catch (IOException e) {
            throw new RuntimeException("Erro ao carregar o esquema Avro", e);
        }
    }

    @KafkaListener(topics = "reservas", groupId = "consumidor-group")
    public void listen(byte[] mensagem) throws IOException {

        ByteArrayInputStream input = new ByteArrayInputStream(mensagem);
        DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
        GenericRecord reservaRecord = reader.read(null, decoder);

        parquetService.setSchema(schema);
        byte[] parquet = parquetService.recordToParquet(reservaRecord);

        // Salvar o arquivo Parquet
        String path = "C:\\Users\\fuchs\\Desktop\\kafka\\dados\\" +
                reservaRecord.get("dataCheckIn").toString() + "\\" +
                reservaRecord.get("nomeHotel").toString() + "\\" +
                reservaRecord.get("statusReserva").toString() + "\\";

        String filePath = path + reservaRecord.get("idReserva").toString() + ".parquet";
        try {
            Path directory = Paths.get(path);
            if (!Files.exists(directory)) {
                Files.createDirectories(directory);
            }

            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                fos.write(parquet);
            }
        } catch (IOException e) {
            System.out.println("Erro ao criar diretório ou escrever no arquivo: " + e.getMessage());
        }

        /*
        String bucketName = "reservas-estagio-fuchs";
        String s3Path = String.format("checkin=%s/hotel=%s/status=%s/%s.parquet",
                reservaRecord.get("dataCheckIn").toString(),
                reservaRecord.get("nomeHotel").toString(),
                reservaRecord.get("statusReserva").toString(),
                reservaRecord.get("idReserva").toString());

        s3UploadService.uploadParquetToS3(parquet, bucketName, s3Path);
        */

    }
}
