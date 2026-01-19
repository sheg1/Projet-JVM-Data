package fr.uha.jvm;

import fr.uha.jvm.model.Game;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.net.URL;
import java.util.Properties;
import java.util.Scanner;

public class CsvPublisher {

    public static void main(String[] args) {
        // Configuration Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer<String, Game> producer = new KafkaProducer<>(props);
        String topic = "games-published";

        try {
            URL resource = CsvPublisher.class.getResource("/vgsales.csv");
            if (resource == null) {
                System.err.println("Fichier introuvable !");
                return;
            }

            File file = new File(resource.toURI());
            Scanner scanner = new Scanner(file);

            if (scanner.hasNextLine()) scanner.nextLine(); // Sauter l'entête

            while (scanner.hasNextLine()) {
                String ligne = scanner.nextLine();
                // Découpe la ligne par les virgules, mais ignore les virgules situées entre guillemets.
                String[] data = ligne.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                Game jeu = Game.newBuilder()
                        .setName(nettoyer(data[0]))
                        .setPlatform(data[1])
                        .setYear(convertirAnnee(data[2]))
                        .setGenre(data[3])
                        .setPublisher(nettoyer(data[4]))
                        .setNaSales(convertirFloat(data[5]))
                        .setEuSales(convertirFloat(data[6]))
                        .setJpSales(convertirFloat(data[7]))
                        .setOtherSales(convertirFloat(data[8]))
                        .setGlobalSales(convertirFloat(data[9]))
                        .setVersion("1.0.0")
                        .setIsEarlyAccess(false)
                        .build();

                // Envoi à Kafka
                ProducerRecord<String, Game> record = new ProducerRecord<>(topic, jeu.getName(), jeu);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Envoyé : " + jeu.getName() + " [" + jeu.getPlatform() + "]");
                    } else {
                        System.err.println("Erreur Kafka : " + exception.getMessage());
                    }
                });
            }

            scanner.close();
            producer.flush();
            producer.close();
            System.out.println("Fin de la publication.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Convertit une chaîne de caractères en un Integer
     */
    private static Integer convertirAnnee(String s) {
        try { return Integer.parseInt(s.trim()); }
        catch (Exception e) { return null; }
    }

    /**
     * Convertit une chaîne de caractères en un float
     */
    private static float convertirFloat(String s) {
        try { return Float.parseFloat(s.trim()); }
        catch (Exception e) { return 0.0f; }
    }

    /**
     * Nettoie le texte pour enlever les espaces et les guillemets superflus.
     */
    private static String nettoyer(String s) {
        return s.trim().replaceAll("^\"|\"$", "");
    }
}