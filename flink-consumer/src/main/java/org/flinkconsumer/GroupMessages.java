package org.flinkconsumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class GroupMessages extends KeyedProcessFunction<String, GenericRecord, Void> {

    private transient ListState<GenericRecord> recordState;
    private transient ParquetConverter parquetConverter;
    private final int batchSize;
    private transient Schema schema;

    private final String schemaString;

    public GroupMessages(String schemaString, int batchSize) {
        this.schemaString = schemaString;
        this.batchSize = batchSize;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        this.schema = new Schema.Parser().parse(schemaString);

        this.parquetConverter = new ParquetConverter();
        parquetConverter.setSchema(schema);

        ListStateDescriptor<GenericRecord> descriptor = new ListStateDescriptor<>(
                "records",
                TypeInformation.of(new TypeHint<GenericRecord>() {})
        );
        recordState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(GenericRecord genericRecord, KeyedProcessFunction<String, GenericRecord, Void>.Context context, Collector<Void> collector) throws Exception {
        recordState.add(genericRecord);

        List<GenericRecord> records = new ArrayList<>();
        for (GenericRecord record : recordState.get()) {
            records.add(record);
        }

        if (records.size() >= batchSize) {
            writeBatchToParquet(records);
            recordState.clear();
        }
    }

    private void writeBatchToParquet(List<GenericRecord> records) {
        try {
            byte[] parquet = parquetConverter.recordsToParquet(records);

            // Salvar o arquivo Parquet
            String path = "/dados/" + records.get(0).get("nomeHotel") + "/";

            String filePath = path + UUID.randomUUID() + ".parquet";
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


            System.out.println("Lote de " + records.size() + " registros foi escrito em Parquet.");
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Erro ao escrever registros em Parquet: " + e.getMessage());
        }
    }
}

