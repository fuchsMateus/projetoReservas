package com.reserva.consumer.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;

@Service
public class S3UploadService {

    @Autowired
    private AmazonS3 s3client;

    public void uploadParquetToS3(byte[] parquetFile, String bucketName, String s3Path) throws IOException {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(parquetFile.length);
        metadata.setContentType("application/octet-stream");

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(parquetFile)) {
            s3client.putObject(bucketName, s3Path, inputStream, metadata);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Erro ao enviar arquivo para o S3", e);
        }
    }
}
