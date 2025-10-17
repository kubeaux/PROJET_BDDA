package minisgbd;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class DBConfigTest {

    // Méthode utilitaire pour créer un fichier de configuration temporaire
    private static void createTempConfigFile(String filename, String content) {
        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("--- Démarrage des tests de DBConfig ---");
        testConstructor();
        testLoadConfigSuccess();
        testLoadConfigFileNotFound();
        testLoadConfigInvalidFormat();
        System.out.println("--- Fin des tests de DBConfig ---");
    }

    private static void testConstructor() {
        System.out.print("Test 1 (Constructeur direct)... ");
        try {
            DBConfig config = new DBConfig("../DB_Data", 4096, 10);
            if (config.getDbpath().equals("../DB_Data")
                    && config.getPagesize() == 4096
                    && config.getDmMaxfilecount() == 10) {
                System.out.println("SUCCÈS");
            } else {
                System.err.println("ÉCHEC : valeurs incorrectes dans le constructeur.");
            }
        } catch (Exception e) {
            System.err.println("ÉCHEC : Exception inattendue.");
            e.printStackTrace();
        }
    }

    private static void testLoadConfigSuccess() {
        System.out.print("Test 2 (Chargement via fichier valide)... ");
        final String TEST_FILE = "test_valid_config.txt";

        createTempConfigFile(TEST_FILE,
                "dbpath = '../DB_Data'\n" +
                "pagesize = 4096\n" +
                "dm_maxfilecount = 10\n");

        try {
            DBConfig config = DBConfig.LoadDBConfig(TEST_FILE);
            if (config.getDbpath().equals("../DB_Data")
                    && config.getPagesize() == 4096
                    && config.getDmMaxfilecount() == 10) {
                System.out.println("SUCCÈS");
            } else {
                System.err.println("ÉCHEC : Valeurs lues incorrectes.");
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
            DBConfig.LoadDBConfig("fichier_inexistant.txt");
            System.err.println("ÉCHEC : aucune exception levée.");
        } catch (IOException e) {
            System.out.println("SUCCÈS (IOException levée)");
        } catch (Exception e) {
            System.err.println("ÉCHEC : Mauvaise exception levée.");
        }
    }

    private static void testLoadConfigInvalidFormat() {
        System.out.print("Test 4 (Format incorrect)... ");
        final String TEST_FILE = "test_invalid_config.txt";

        // fichier volontairement invalide : il manque des champs
        createTempConfigFile(TEST_FILE, "dbpath = '../DB_Data'");

        try {
            DBConfig.LoadDBConfig(TEST_FILE);
            System.err.println("ÉCHEC : aucune exception levée.");
        } catch (IllegalArgumentException e) {
            System.out.println("SUCCÈS (IllegalArgumentException levée)");
        } catch (Exception e) {
            System.err.println("ÉCHEC : Mauvaise exception levée.");
        } finally {
            new File(TEST_FILE).delete();
        }
    }
}
