package com.reserva.consumer.service;

import com.reserva.consumer.model.Reserva;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Service;

@Service
public class ReservaService extends ParquetService {

    @Override
    public GenericRecord convertToAvroRecord(Object object) {
        GenericRecord record = new GenericData.Record(this.getSchema());
        Reserva reserva = (Reserva) object;
        record.put("idReserva", reserva.getIdReserva().toString());
        record.put("nomeHospede", reserva.getNomeHospede());
        record.put("emailHospede", reserva.getEmailHospede());
        record.put("dataCheckIn", (int) reserva.getDataCheckIn().toEpochDay());
        record.put("dataCheckOut", (int) reserva.getDataCheckOut().toEpochDay());
        record.put("statusReserva", reserva.getStatusReserva());
        record.put("precoTotal", reserva.getPrecoTotal());
        record.put("tipoQuarto", reserva.getTipoQuarto());
        record.put("telefoneContato", reserva.getTelefoneContato());
        return record;
    }
}
