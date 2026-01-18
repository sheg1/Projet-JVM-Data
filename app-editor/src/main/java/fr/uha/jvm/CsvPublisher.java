package fr.uha.jvm;

import fr.uha.jvm.model.Game;
import java.io.File;
import java.util.Scanner;
import java.net.URL;

public class CsvPublisher {

    public static void main(String[] args) {
        try {
            URL resource = CsvPublisher.class.getResource("/vgsales.csv");
            if (resource == null) {
                System.err.println("Fichier introuvable !");
                return;
            }

            File file = new File(resource.toURI());
            Scanner scanner = new Scanner(file);

            if (scanner.hasNextLine()) scanner.nextLine(); // Sauter l'en-tête

            while (scanner.hasNextLine()) {
                String ligne = scanner.nextLine();

                // Ne pas couper les virgules qui sont entre guillemets ""
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

                System.out.println("Ok : " + jeu.getName() + " (" + jeu.getPublisher() + ")");
            }
            scanner.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Gérer le cas où l'année est N/A
    private static Integer convertirAnnee(String s) {
        try { return Integer.parseInt(s.trim()); }
        catch (Exception e) { return null; }
    }

    // Pour éviter le crash "For input string" si les données sont mal décalées
    private static float convertirFloat(String s) {
        try { return Float.parseFloat(s.trim()); }
        catch (Exception e) { return 0.0f; }
    }

    // Enlever les guillemets autour des noms si nécessaire
    private static String nettoyer(String s) {
        return s.trim().replaceAll("^\"|\"$", "");
    }
}