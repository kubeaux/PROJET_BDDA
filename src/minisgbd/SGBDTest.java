package minisgbd;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class SGBDTest {

    private static final String CONFIG =
            "C:/Users/Admin/Desktop/PROJET_BDDA/config.txt";

    public static void main(String[] args) throws Exception {
        System.out.println("===== TEST TP6 SGBD =====");

        // Nettoyage : supprimer DB_Data/BinData
        cleanDirectory("C:/Users/Admin/Desktop/PROJET_BDDA/DB_Data/BinData");

        // Lancer le SGBD en mode interactif via ProcessBuilder
        ProcessBuilder pb = new ProcessBuilder(
                "java", "-cp", "bin", "minisgbd.SGBD", CONFIG
        );
        pb.redirectErrorStream(true);

        Process process = pb.start();
        OutputStream os = process.getOutputStream();
        BufferedWriter writer =
                new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));

        InputStream is = process.getInputStream();
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));

        // Envoyer les commandes du TP6
        send(writer, "DROP TABLES");
        send(writer, "CREATE TABLE Tab1 (C1:FLOAT,C2:INT)");
        send(writer, "DESCRIBE TABLES");
        send(writer, "CREATE TABLE Tab2 (C7:CHAR(5),AA:VARCHAR(2))");
        send(writer, "DESCRIBE TABLES");
        send(writer, "DROP TABLE Tab1");
        send(writer, "DESCRIBE TABLES");
        send(writer, "EXIT");

        // Lire la sortie du SGBD
        Thread.sleep(500); // attendre la sortie
        String line;
        System.out.println("===== SORTIE DU SGBD =====");
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        process.waitFor();
        System.out.println("===== FIN TEST TP6 =====");
    }

    /** envoi d’une commande au SGBD */
    private static void send(BufferedWriter w, String cmd) throws Exception {
        w.write(cmd);
        w.newLine();
        w.flush();
        System.out.println(">>> " + cmd);
    }

    /** suppression d’un dossier */
    private static void cleanDirectory(String path) {
        File dir = new File(path);
        if (!dir.exists()) return;
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) f.delete();
    }
}
