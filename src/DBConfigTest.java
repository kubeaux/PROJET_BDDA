package minisgbd;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class DBConfigTest {

    // Crée un fichier de configuration temporaire pour les tests
    private static void createTempConfigFile(String filename, String content) {
        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void runTests() {
        System.out.println("--- Démarrage des tests de DBConfig ---");
        
        // G. Test 1 : Construction en mémoire (Constructeur)
        testConstructor();
        
        // G. Test 2 : Chargement via Fichier (Cas Succès)
        testLoadConfigSuccess();
        
        // G. Test 3 : Gestion des Erreurs (Fichier manquant)
        testLoadConfigFileNotFound();
        
        // G. Test 4 : Gestion des Erreurs (Format incorrect)
        testLoadConfigInvalidFormat();
        
        System.out.println("--- Fin des tests de DBConfig ---");
    }

    private static void testConstructor() {
        System.out.print("Test 1 (Constructeur direct)... ");
        try {
            DBConfig config = new DBConfig("/my/test/path");
            if (config.getDbpath().equals("/my/test/path")) {
                System.out.println("SUCCÈS");
            } else {
                System.err.println("ÉCHEC : Chemin incorrect.");
            }
        } catch (Exception e) {
            System.err.println("ÉCHEC : Exception inattendue.");
            e.printStackTrace();
        }
    }

    private static void testLoadConfigSuccess() {
        System.out.print("Test 2 (Chargement par fichier)... ");
        final String TEST_FILE = "test_valid_config.txt";
        createTempConfigFile(TEST_FILE, "dbpath = 'C:/DB_PROD'");

        try {
            DBConfig config = DBConfig.LoadDBConfig(TEST_FILE);
            if (config.getDbpath().equals("C:/DB_PROD")) {
                System.out.println("SUCCÈS");
            } else {
                System.err.println("ÉCHEC : Valeur lue incorrecte: " + config.getDbpath());
            }
        } catch (Exception e) {
            System.err.println("ÉCHEC : Exception inattendue.");
            e.printStackTrace();
        } finally {
            new File(TEST_FILE).delete();
        }
    }

    private static void testLoadConfigFileNotFound() {
        System.out.print("Test 3 (Fichier manquant)... ");
        try {
            DBConfig.LoadDBConfig("un_fichier_qui_nexiste_pas.txt");
            System.err.println("ÉCHEC : Aucune exception levée.");
        } catch (IOException e) {
            System.out.println("SUCCÈS (IOException levée)");
        } catch (Exception e) {
            System.err.println("ÉCHEC : Mauvaise exception levée.");
        }
    }
    
    private static void testLoadConfigInvalidFormat() {
        System.out.print("Test 4 (Format incorrect)... ");
        final String TEST_FILE = "test_bad_format_config.txt";
        createTempConfigFile(TEST_FILE, "nimporte_quoi"); // dbpath manquant

        try {
            DBConfig.LoadDBConfig(TEST_FILE);
            System.err.println("ÉCHEC : Aucune exception levée.");
        } catch (IllegalArgumentException e) {
            System.out.println("SUCCÈS (IllegalArgumentException levée)");
        } catch (Exception e) {
            System.err.println("ÉCHEC : Mauvaise exception levée.");
        } finally {
            new File(TEST_FILE).delete();
        }
    }
}