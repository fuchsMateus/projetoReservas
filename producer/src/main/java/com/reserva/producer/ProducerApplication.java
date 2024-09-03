package com.reserva.producer;

import com.reserva.producer.model.Reserva;
import com.reserva.producer.model.ReservaFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@ComponentScan(basePackages = "com.reserva.producer")
public class ProducerApplication {

	private static KafkaTemplate<String, Reserva> kafkaTemplate;
	public ProducerApplication(KafkaTemplate<String, Reserva> kafkaTemplate ) {
		ProducerApplication.kafkaTemplate = kafkaTemplate;
	}

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);

		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

		Runnable tarefa = () -> {

			for (int i = 0; i < 1; i++) {
				Reserva reservaAleatoria = ReservaFactory.gerarReservaAleatoria();
				kafkaTemplate.send("reservas", reservaAleatoria.getIdReserva().toString(), reservaAleatoria);
			}

			System.out.println("Lote de mensagens enviado");
		};

		scheduler.scheduleAtFixedRate(tarefa, 0, 1, TimeUnit.SECONDS);
	}

}
