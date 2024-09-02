package com.reserva.consumer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDate;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Reserva {
    private UUID idReserva;
    private String nomeHospede;
    private String emailHospede;
    private LocalDate dataCheckIn;
    private LocalDate dataCheckOut;
    private String statusReserva;
    private Double precoTotal;
    private String tipoQuarto;
    private String telefoneContato;
}
