import java.io.BufferedReader;
import java.io.FileReader;

public class DBConfig {
    private String dbpath;
    private int pageSize;
    private int dm_maxFileCount;
    private int dm_maxPagePerFile;
    private int bm_bufferCount;
    private String bm_policy;

    public DBConfig(String dbpath, int pageSize, int dm_maxFileCount, int dm_maxPagePerFile,
                    int bm_bufferCount, String bm_policy) {
        this.dbpath = dbpath;
        this.pageSize = pageSize;
        this.dm_maxFileCount = dm_maxFileCount;
        this.dm_maxPagePerFile = dm_maxPagePerFile;
        this.bm_bufferCount = bm_bufferCount;
        this.bm_policy = bm_policy;
    }

    // Getters
    public String getDbPath() {
        return dbpath;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getDmMaxFileCount() {
        return dm_maxFileCount;
    }

    public int getDmMaxPagePerFile() {
        return dm_maxPagePerFile;
    }

    public int getBmBufferCount() {
        return bm_bufferCount;
    }

    public String getBmPolicy() {
        return bm_policy;
    }

    public static DBConfig loadDBConfig(String fichierConfig) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(fichierConfig));
        String line;
        String dbpath = null;
        Integer pageSize = null;
        Integer dm_maxFileCount = null;
        Integer dm_maxPagePerFile = null;
        Integer bm_bufferCount = 10; // Valeur par défaut
        String bm_policy = "LRU";    // Valeur par défaut

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue; // Ignorer les lignes vides ou commentaires

            String[] parts = line.split("=");
            if (parts.length != 2) continue;

            String key = parts[0].trim().toLowerCase();
            String value = parts[1].trim();

            switch (key) {
                case "dbpath":
                    dbpath = value;
                    break;
                case "pageSize":
                    pageSize = Integer.parseInt(value);
                    break;
                case "dm_maxFileCount":
                    dm_maxFileCount = Integer.parseInt(value);
                    break;
                case "dm_maxPagePerFile":
                    dm_maxPagePerFile = Integer.parseInt(value);
                    break;
                case "bm_bufferCount":
                    bm_bufferCount = Integer.parseInt(value);
                    break;
                case "bm_policy":
                    bm_policy = value.toUpperCase();
                    break;
            }
        }
        reader.close();

        // Vérification des champs obligatoires
        if (dbpath == null || pageSize == null || dm_maxFileCount == null || dm_maxPagePerFile == null) {
            throw new Exception("Paramètres manquants dans le fichier de configuration : " + fichierConfig);
        }

        return new DBConfig(dbpath, pageSize, dm_maxFileCount, dm_maxPagePerFile, bm_bufferCount, bm_policy);
    }

    @Override
    public String toString() {
        return "DBConfig{" +
                "dbpath='" + dbpath + '\'' +
                ", pageSize=" + pageSize +
                ", dm_maxFileCount=" + dm_maxFileCount +
                ", dm_maxPagePerFile=" + dm_maxPagePerFile +
                ", bm_bufferCount=" + bm_bufferCount +
                ", bm_policy='" + bm_policy + '\'' +
                '}';
    }
}
