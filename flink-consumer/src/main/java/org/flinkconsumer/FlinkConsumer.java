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
import java.io.InputStream;

public class FlinkConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        InputStream avroSchemaStream = FlinkConsumer.class.getClassLoader().getResourceAsStream("avro/reserva.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaStream);


        KafkaSource<GenericRecord> kafkaSource = KafkaSource.<GenericRecord>builder()
                .setBootstrapServers("kafka:9093")
                .setGroupId("flink-group")
                .setTopics("reservas")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(AvroDeserializationSchema.forGeneric(schema))
                .build();

        DataStream<GenericRecord> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream.keyBy(record -> record.get("nomeHotel").toString())
                .process(new GroupMessages(schema.toString(), 2));

        //stream.print();

        env.execute("FlinkConsumer");

    }
}
