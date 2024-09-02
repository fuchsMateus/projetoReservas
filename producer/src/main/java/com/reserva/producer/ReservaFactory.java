package com.reserva.producer;

import java.text.Normalizer;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class ReservaFactory {

    private static final Random random = new Random();

    private static final List<String> NOMES = Arrays.asList(
            "João", "Maria", "Pedro", "Ana", "Lucas", "Mariana", "Gustavo", "Isabela", "Gabriel", "Fernanda",
            "Rafael", "Camila", "Bruno", "Juliana", "Felipe", "Carolina", "Thiago", "Larissa", "Eduardo", "Aline",
            "Rodrigo", "Renata", "André", "Beatriz", "Marcelo", "Tatiana", "Leandro", "Vanessa", "Ricardo", "Patrícia",
            "Mateus", "Jorge", "Leticia", "Valéria", "Giovane", "Wagner", "Lívia", "Heloisa", "Marcos", "Vitor",
            "Maisa", "Bianca", "Julia", "Lara", "Carol", "Alan", "Guilherme", "William", "Luís", "Samuel",
            "Henrique", "Clara", "Elisa", "Daniel", "Cecília", "Murilo", "Sofia", "Otávio", "Mirella", "Jonathan",
            "Sara", "Renato", "Helena", "Eduarda", "Arthur", "Lorena", "Nicolas", "Paula", "Cauã", "Fabiana",
            "Matheus", "Sabrina", "Diego", "Ingrid", "Alex", "Tainá", "Elias", "Débora", "Vinícius", "Carla",
            "Guilhermina", "Marcos", "Tatiane", "Enzo", "Raquel", "Leonardo", "Michelle", "Augusto", "Emanuele",
            "Caio", "Lorraine", "Antonio", "Adriana", "Caetano", "Valentina", "Joaquim", "Mônica", "Emílio", "Graziela"
    );

    private static final List<String> SOBRENOMES = Arrays.asList(
            "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Almeida", "Costa", "Gomes", "Martins",
            "Araújo", "Lima", "Barbosa", "Ribeiro", "Pereira", "Carvalho", "Melo", "Cardoso", "Castro", "Reis",
            "Moreira", "Dias", "Teixeira", "Correia", "Cavalcanti", "Monteiro", "Moura", "Nunes", "Vieira", "Campos",
            "Irala", "Faria", "Peralta", "Pedroso", "Viana", "Batista", "Gonçalves", "Muller", "Cardinal", "Miranda",
            "Calisto", "Amorim", "Mendonça", "Andrade", "Machado", "Assis", "Rocha", "Benites", "Ramalho", "Bezerra",
            "Franco", "Matos", "Serra", "Menezes", "Peixoto", "Barreto", "Freitas", "Antunes", "Cunha", "Furtado",
            "Garcia", "Macedo", "Cruz", "Neves", "Pires", "Ramos", "Silveira", "Nogueira", "Fonseca", "Marinho",
            "Azevedo", "Coelho", "Duarte", "Leal", "Macedo", "Queiroz", "Silvestre", "Xavier", "Carvalho", "Pinto",
            "Monteiro", "Esteves", "Bernardes", "Ribeiro", "Seabra", "Siqueira", "Galvão", "Tavares", "Valle", "Pimenta",
            "Torres", "Souto", "Fonseca", "Veiga", "Lacerda", "Drumond", "Barros", "Cabral", "Campos", "Lemos"
    );


    private static final List<String> TIPOS_QUARTO = Arrays.asList(
            "Solteiro", "Solteiro", "Solteiro", "Duplo", "Duplo", "Duplo","Duplo", "Suíte","Suíte", "Presidencial"
    );

    private static final List<String> STATUS_RESERVA = Arrays.asList(
            "Confirmada", "Confirmada", "Cancelada"
    );

    private static final List<String> DOMINIOS_EMAIL = Arrays.asList(
            "gmail.com", "gmail.com", "gmail.com", "gmail.com", "outlook.com", "outlook.com", "hotmail.com", "hotmail.com", "yahoo.com"
    );

    private static final List<Integer> DIAS = Arrays.asList(
            1,1,1,1,1,1,2,2,3,3,3,3,4,4,5,5,5,5,5,6,7,7,7,7,8,9,10,10,11,12,13,14,14,14
    );

    public static Reserva gerarReservaAleatoria() {
        UUID idReserva = UUID.randomUUID();
        String nomeHospede = gerarNomeAleatorio();
        String emailHospede = gerarEmailAleatorio(nomeHospede);
        LocalDate dataCheckIn = gerarDataAleatoria();
        int numeroDias = gerarDiaAleatorio();
        LocalDate dataCheckOut = dataCheckIn.plusDays(numeroDias);
        String statusReserva = STATUS_RESERVA.get(random.nextInt(STATUS_RESERVA.size()));
        String tipoQuarto = TIPOS_QUARTO.get(random.nextInt(TIPOS_QUARTO.size()));
        Double precoTotal = calcularPrecoTotal(tipoQuarto,numeroDias);
        String telefoneContato = gerarTelefoneAleatorio();

        return new Reserva(idReserva, nomeHospede, emailHospede, dataCheckIn, dataCheckOut, statusReserva, precoTotal, tipoQuarto, telefoneContato);
    }

    private static double calcularPrecoTotal(String tipoQuarto, Integer numeroDias) {
        double precoBasePorDia = switch (tipoQuarto) {
            case "Duplo" -> 150.0;
            case "Suíte" -> 250.0;
            case "Presidencial" -> 400.0;
            default -> 100.0;
        };

        return precoBasePorDia * numeroDias;
    }

    private static String gerarNomeAleatorio() {
        String primeiroNome = NOMES.get(random.nextInt(NOMES.size()));
        String sobrenome = SOBRENOMES.get(random.nextInt(SOBRENOMES.size()));
        return primeiroNome + " " + sobrenome;
    }

    private static Integer gerarDiaAleatorio() {
        return DIAS.get(random.nextInt(DIAS.size()));
    }

    private static String gerarEmailAleatorio(String nomeHospede) {
        String dominio = DOMINIOS_EMAIL.get(random.nextInt(DOMINIOS_EMAIL.size()));
        return removerAcentosECedilha(nomeHospede.toLowerCase().replace(" ", "")) + "@" + dominio;
    }

    private static LocalDate gerarDataAleatoria() {
        int chance = random.nextInt(100);
        Month mes;
        if (chance < 50) {
            mes = switch (random.nextInt(3)) {
                case 1 -> Month.JULY;
                case 2 -> Month.DECEMBER;
                default -> Month.JANUARY;
            };
        } else {
            mes = Month.of(random.nextInt(12) + 1);
        }

        int ano = LocalDate.now().getYear() + random.nextInt(3) - 1;
        int dia = random.nextInt(mes.length(Year.isLeap(ano))) + 1;

        return LocalDate.of(ano, mes, dia);
    }

    public static String removerAcentosECedilha(String texto) {
        String normalizado = Normalizer.normalize(texto, Normalizer.Form.NFD);
        String semAcentos = normalizado.replaceAll("\\p{M}", "");
        return semAcentos.replaceAll("ç", "c").replaceAll("Ç", "C");
    }

    private static String gerarTelefoneAleatorio() {

        int chance = random.nextInt(100);
        int ddd;
        if (chance < 30) {
            ddd = 67;
        } else {
            ddd = random.nextInt(89) + 11; // DDD entre 11 e 99
        }

        int parte2 = random.nextInt(10000);
        int parte3 = random.nextInt(10000);
        return String.format("(%02d) 9%04d-%04d", ddd, parte2, parte3);
    }


}
