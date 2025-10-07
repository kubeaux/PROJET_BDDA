import java.io.BufferedReader;
import java.io.FileReader;

public class DBConfig {
    private String dbpath;
    private byte pagesize;
    private int dm_maxfilecount;

    public DBConfig(String dbpath, byte pagesize, int dm_maxfilecount) {
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
    }

    // Getters et setters

    public byte getPagesize(){
        return pagesize;
    }

    public int getDmMaxfilecount(){
        return dm_maxfilecount;
    }

    public void setPagesize(byte pagesize){
        this.pagesize = pagesize;
    }

    public void setDmMaxfilecount(int dm_maxfilecount){
        this.dm_maxfilecount = dm_maxfilecount;
    }

    public static DBConfig loadDBConfig(String fichierConfig) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(fichierConfig));
        String line;
        String dbpath = null;
        Byte pagesize = null;
        Integer dm_maxfilecount = null;

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("dbpath")) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    dbpath = parts[1].trim();
                }
            } else if (line.startsWith("pagesize")) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    pagesize = Byte.parseByte(parts[1].trim());
                }
            } else if (line.startsWith("dm_maxfilecount")) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    dm_maxfilecount = Integer.parseInt(parts[1].trim());
                }
            }
        }
        reader.close();

        if (dbpath == null || pagesize == null || dm_maxfilecount == null) {
            throw new Exception("Paramètres manquants dans le fichier de configuration : " + fichierConfig);
        }

        DBConfig config = new DBConfig(dbpath, pagesize, dm_maxfilecount);
        return config;
    }


    @Override
    public String toString() {
        return "DBConfig{" +
                "dbpath='" + dbpath + '\'' +
                ", pagesize=" + pagesize +
                ", dm_maxfilecount=" + dm_maxfilecount +
                '}';
    }

}
