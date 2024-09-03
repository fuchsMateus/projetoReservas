package com.reserva.consumer.kafkalistener;

import com.reserva.consumer.model.Reserva;
import com.reserva.consumer.service.ReservaService;
import com.reserva.consumer.service.S3UploadService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


@Component
public class ReservaListener {

    private final ReservaService reservaService;
    private final S3UploadService s3UploadService;

    public ReservaListener(ReservaService reservaService, S3UploadService s3UploadService) {
        this.reservaService = reservaService;
        this.s3UploadService = s3UploadService;
    }

    @KafkaListener(topics = "reservas", groupId = "consumidor-group")
    public void listen(Reserva reserva) throws IOException {

        System.out.println("Mensagem Recebida: " + reserva);

        Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/reserva.avsc"));
        reservaService.setSchema(schema);
        byte[] parquet =  reservaService.recordToParquet(reservaService.convertToAvroRecord(reserva));
        /*
        String path = "C:\\Users\\fuchs\\Desktop\\kafka\\dados\\"+reserva.getIdReserva().toString()+".parquet";

        try (FileOutputStream fos = new FileOutputStream(path)) {
            fos.write(parquet);
        } catch (IOException e) {
            System.out.println("Erro ao escrever no arquivo: " + e.getMessage());
        }*/


        String bucketName = "reservas-estagio-fuchs";
        String s3Path = String.format("checkin=%s/status=%s/%s.parquet",
                reserva.getDataCheckIn().toString(), reserva.getStatusReserva(), reserva.getIdReserva().toString());

        s3UploadService.uploadParquetToS3(parquet, bucketName, s3Path);

    }
}
