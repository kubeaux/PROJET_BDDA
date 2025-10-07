
public class TestDBConfig {
    public static void main() {
        try {
            // === Test 1 : Création directe ===
            DBConfig config1 = new DBConfig("../BinData/",64,16, 2048);

            System.out.println("Test 1 (constructeur direct) :");
            System.out.println(config1);

            // === Test 2 : Création via fichier de config ===
            DBConfig config2 = DBConfig.loadDBConfig("config.txt");
            System.out.println("\nTest 2 (lecture depuis config.txt) :");
            System.out.println(config2);

            // === Test 3 : Fichier inexistant ===
            try {
                DBConfig config3 = DBConfig.loadDBConfig("fichier_inexistant.txt");
                System.out.println("\nTest 3 (ERREUR ATTENDUE) : ce test aurait dû échouer !");
                System.out.println(config3);
            } catch (Exception e) {
                System.out.println("\nTest 3 (fichier inexistant) : OK, erreur capturée : " + e.getMessage());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
