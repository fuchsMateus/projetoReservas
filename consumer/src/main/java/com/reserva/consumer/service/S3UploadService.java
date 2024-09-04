package com.reserva.consumer.service;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class S3UploadService {

    @Autowired
    private S3Client s3client;

    public void uploadParquetToS3(byte[] parquetFile, String bucketName, String s3Path) throws IOException {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Path)
                    .contentType("application/octet-stream")
                    .contentLength((long) parquetFile.length)
                    .build();

            s3client.putObject(putObjectRequest, RequestBody.fromBytes(parquetFile));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Erro ao enviar arquivo para o S3", e);
        }
    }
}
