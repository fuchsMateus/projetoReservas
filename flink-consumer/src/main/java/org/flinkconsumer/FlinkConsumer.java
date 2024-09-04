package org.flinkconsumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class FlinkConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint para garantir tolerância a falhas
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/reserva.avsc"));

        KafkaSource<GenericRecord> kafkaSource = KafkaSource.<GenericRecord>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("flink-group")
                .setTopics("reservas")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(AvroDeserializationSchema.forGeneric(schema))
                .build();

        DataStream<GenericRecord> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream.keyBy(record -> record.get("nomeHotel").toString())
                .process(new GroupMessages(schema.toString(), 50));

        stream.print();

        env.execute("FlinkConsumer");

    }
}
