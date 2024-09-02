package com.reserva.consumer;

import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.IOException;


@Component
public class ReservaListener {

    private final ParquetService parquetService;
    private final S3UploadService s3UploadService;

    public ReservaListener(ParquetService parquetService, S3UploadService s3UploadService) {
        this.parquetService = parquetService;
        this.s3UploadService = s3UploadService;
    }

    @KafkaListener(topics = "reservas", groupId = "consumidor-group")
    public void listen(Reserva reserva) throws IOException {
        //System.out.println("Mensagem Recebida: " + reserva);
        byte[] parquet = parquetService.getReservaParquet(reserva);

        String path = "C:\\Users\\fuchs\\Desktop\\kafka\\dados\\"+reserva.getIdReserva().toString()+".parquet";

        try (FileOutputStream fos = new FileOutputStream(path)) {
            fos.write(parquet);
        } catch (IOException e) {
            System.out.println("Erro ao escrever no arquivo: " + e.getMessage());
        }

        /*
        String bucketName = "reservas-estagio-fuchs";
        String s3Path = String.format("checkin=%s/status=%s/%s.parquet",
                reserva.getDataCheckIn().toString(), reserva.getStatusReserva(), reserva.getIdReserva().toString());

        s3UploadService.uploadParquetToS3(parquet, bucketName, s3Path);
        */
    }
}
