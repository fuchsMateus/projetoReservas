package com.reserva.producer;

import com.reserva.producer.model.Reserva;
import com.reserva.producer.model.ReservaFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@ComponentScan(basePackages = "com.reserva.producer")
public class ProducerApplication {

	private static KafkaTemplate<String, byte[]> kafkaTemplate;
	private static final String TOPICO = "reservas";
	private static Schema schema;
	private final String SCHEMA_PATH = "src/main/resources/avro/reserva.avsc";
	public ProducerApplication(KafkaTemplate<String, byte[]> kafkaTemplate ) {
		ProducerApplication.kafkaTemplate = kafkaTemplate;
		try{
			schema = new Schema.Parser().parse(new File(SCHEMA_PATH));
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
		produzirMensagens(2,1,true);
	}

	public static void produzirMensagens(int numMensagens, int periodo, boolean log){
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

		Runnable tarefa = () -> {

			for (int i = 0; i < numMensagens; i++) {
				Reserva reservaAleatoria = ReservaFactory.gerarReservaAleatoria();

				try {
					GenericRecord record = new GenericData.Record(schema);
					record.put("idReserva", reservaAleatoria.getIdReserva().toString());
					record.put("nomeHotel", reservaAleatoria.getNomeHotel());
					record.put("nomeHospede", reservaAleatoria.getNomeHospede());
					record.put("emailHospede", reservaAleatoria.getEmailHospede());
					record.put("dataCheckIn", reservaAleatoria.getDataCheckIn().toString());
					record.put("dataCheckOut", reservaAleatoria.getDataCheckOut().toString());
					record.put("statusReserva", reservaAleatoria.getStatusReserva());
					record.put("precoTotal", reservaAleatoria.getPrecoTotal());
					record.put("tipoQuarto", reservaAleatoria.getTipoQuarto());
					record.put("telefoneContato", reservaAleatoria.getTelefoneContato());

					ByteArrayOutputStream out = new ByteArrayOutputStream();
					DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
					Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
					writer.write(record, encoder);
					encoder.flush();
					out.close();

					byte[] bytes = out.toByteArray();

					kafkaTemplate.send(TOPICO, reservaAleatoria.getNomeHotel(), bytes);
				}catch (Exception e){
					e.printStackTrace();
				}
			}

			if (log) System.out.println("Lote de mensagens enviado");
		};

		scheduler.scheduleAtFixedRate(tarefa, 0, periodo, TimeUnit.SECONDS);
	}


}
