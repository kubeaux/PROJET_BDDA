import java.io.BufferedReader;
import java.io.FileReader;

public class DBConfig {
    private String dbpath;
    private int pageSize;
    private int dm_maxFileCount;
    private int bm_bufferCount;
    private String bm_policy;

    public DBConfig(String dbpath, int pageSize, int dm_maxFileCount, int bm_bufferCount, String bm_policy) {
        this.dbpath = dbpath;
        this.pageSize = pageSize;
        this.dm_maxFileCount = dm_maxFileCount;
        this.bm_bufferCount = bm_bufferCount;
        this.bm_policy = bm_policy;
    }

    public String getDbPath() {
        return dbpath;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getDm_maxFileCount() {
        return dm_maxFileCount;
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
        int pageSize = 0;
        int dm_maxFileCount = 0;
        int bm_buffercount = 0;
        String bm_policy = "LRU"; // valeur par défaut

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("dbpath")) {
                dbpath = line.split("=")[1].trim();
            } else if (line.startsWith("pageSize")) {
                pageSize = Integer.parseInt(line.split("=")[1].trim());
            } else if (line.startsWith("dm_maxFileCount")) {
                dm_maxFileCount = Integer.parseInt(line.split("=")[1].trim());
            } else if (line.startsWith("bm_buffercount")) {
                bm_buffercount = Integer.parseInt(line.split("=")[1].trim());
            } else if (line.startsWith("bm_policy")) {
                bm_policy = line.split("=")[1].trim();
            }
        }
        reader.close();

        if (dbpath == null || pageSize == 0 || dm_maxFileCount == 0) {
            throw new Exception("Configuration invalide dans " + fichierConfig);
        }

        return new DBConfig(dbpath, pageSize, dm_maxFileCount, bm_buffercount, bm_policy);
    }

    @Override
    public String toString() {
        return "DBConfig{" +
                "dbpath='" + dbpath + '\'' +
                ", pageSize=" + pageSize +
                ", dm_maxFileCount=" + dm_maxFileCount +
                ", bm_buffercount=" + bm_bufferCount +
                ", bm_policy='" + bm_policy + '\'' +
                '}';
    }
}



