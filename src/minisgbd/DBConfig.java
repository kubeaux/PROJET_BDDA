package minisgbd;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DBConfig {
    private final String dbpath;
    private final int pagesize;
    private final int dm_maxfilecount;
    private final int bm_buffercount;
    private final String bm_policy;

    // Constructeur complet
    public DBConfig(String dbpath, int pagesize, int dm_maxfilecount,
                    int bm_buffercount, String bm_policy) {
        if (dbpath == null || dbpath.isEmpty()) throw new IllegalArgumentException("dbpath invalide");
        if (pagesize <= 0) throw new IllegalArgumentException("pagesize doit être positif");
        if (dm_maxfilecount <= 0) throw new IllegalArgumentException("dm_maxfilecount doit être positif");
        if (bm_buffercount <= 0) throw new IllegalArgumentException("bm_buffercount doit être positif");
        if (bm_policy == null || bm_policy.isEmpty()) throw new IllegalArgumentException("bm_policy invalide");

        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
        this.bm_buffercount = bm_buffercount;
        this.bm_policy = bm_policy;
    }

    // Getters
    public String getDbpath() { return dbpath; }
    public int getPagesize() { return pagesize; }
    public int getDm_maxfilecount() { return dm_maxfilecount; }
    public int getBm_buffercount() { return bm_buffercount; }
    public String getBm_policy() { return bm_policy; }

    // Charger depuis un fichier Properties
    public static DBConfig loadDBConfig(String configFile) throws IOException {
        Properties p = new Properties();

        File f = new File(configFile);
        if (!f.exists()) throw new IOException("Config file not found: " + configFile);

        try (FileInputStream fis = new FileInputStream(f)) {
            p.load(fis);
        }

        String dbpath = p.getProperty("dbpath");
        int pagesize = Integer.parseInt(p.getProperty("pagesize"));
        int maxfile = Integer.parseInt(p.getProperty("dm_maxfilecount"));
        int bm_buffercount = Integer.parseInt(p.getProperty("bm_buffercount", "5")); // défaut 5
        String bm_policy = p.getProperty("bm_policy", "LRU"); // défaut LRU

        return new DBConfig(dbpath, pagesize, maxfile, bm_buffercount, bm_policy);
    }

    @Override
    public String toString() {
        return "DBConfig{ dbpath=" + dbpath +
               ", pagesize=" + pagesize +
               ", dm_maxfilecount=" + dm_maxfilecount +
               ", bm_buffercount=" + bm_buffercount +
               ", bm_policy=" + bm_policy + " }";
    }
}
