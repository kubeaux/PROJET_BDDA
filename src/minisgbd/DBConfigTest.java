package minisgbd;

public class DBConfigTest {
    public static void main(String[] args) {
        try {
            // Test constructeur direct
            DBConfig cfg1 = new DBConfig("testdb", 4096, 10, 5, "LRU");
            System.out.println("Constructeur direct : " + cfg1);

            // Test lecture depuis fichier
            // Crée un fichier config.properties avec :
            // dbpath = ../DB_Data
            // pagesize = 4096
            // dm_maxfilecount = 10
            // bm_buffercount = 5
            // bm_policy = LRU
            DBConfig cfg2 = DBConfig.loadDBConfig("config.properties");
            System.out.println("Chargé depuis fichier : " + cfg2);

            // Vérifier les getters
            assert cfg2.getDbpath().equals("../DB_Data");
            assert cfg2.getPagesize() == 4096;
            assert cfg2.getDm_maxfilecount() == 10;
            assert cfg2.getBm_buffercount() == 5;
            assert cfg2.getBm_policy().equals("LRU");

            System.out.println("=== DBConfigTests OK ===");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

