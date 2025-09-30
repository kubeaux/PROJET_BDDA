public class TestDBConfig {
    public static void main(String[] args) {
        try {
            // Test 1 : création directe
            DBConfig config1 = new DBConfig("../DB");
            System.out.println("Test 1 (constructeur direct) : " + config1);

            // Test 2 : création via fichier
            DBConfig config2 = DBConfig.loadDBConfig("config.txt");
            System.out.println("Test 2 (fichier config.txt) : " + config2);

            // Test 3 : cas d'erreur
            try {
                DBConfig config3 = DBConfig.loadDBConfig("fichier_inexistant.txt");
                System.out.println(config3);
            } catch (Exception e) {
                System.out.println("Test 3 (erreur attendue) : " + e.getMessage());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
