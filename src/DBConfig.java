import java.io.BufferedReader;
import java.io.FileReader;

public class DBConfig {
    private String dbpath;
    private int pagesize;
    private int dm_maxfilecount;
    private int dm_maxpageperfile;

    // public DBConfig(String dbpath, byte pagesize, int dm_maxfilecount) {
    //     this.dbpath = dbpath;
    //     this.pagesize = pagesize;
    //     this.dm_maxfilecount = dm_maxfilecount;
    // }

    public DBConfig(String dbpath, int pagesize, int dm_maxfilecount, int dm_maxpageperfile) {
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
        this.dm_maxpageperfile = dm_maxpageperfile;
    }

    // Getters et setters

    public String getDbpath(){
        return dbpath;
    }

    public int getPagesize(){
        return pagesize;
    }

    public int getDmMaxfilecount(){
        return dm_maxfilecount;
    }

    public int getDmMaxpageperfile(){
        return dm_maxpageperfile;
    }

    public void setPagesize(byte pagesize){
        this.pagesize = pagesize;
    }

    public void setDmMaxfilecount(int dm_maxfilecount){
        this.dm_maxfilecount = dm_maxfilecount;
    }

    public void setDmMaxpageperfile(int dm_maxpageperfile){
        this.dm_maxpageperfile = dm_maxpageperfile;
    }

    public static DBConfig loadDBConfig(String fichierConfig) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(fichierConfig));
        String line;
        String dbpath = null;
        Integer pagesize = null;
        Integer dm_maxfilecount = null;
        Integer dm_maxpageperfile = null;

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
                    pagesize = Integer.parseInt(parts[1].trim());
                }
            } else if (line.startsWith("dm_maxfilecount")) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    dm_maxfilecount = Integer.parseInt(parts[1].trim());
                }
            } else if (line.startsWith("dm_maxpageperfile")) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    dm_maxpageperfile = Integer.parseInt(parts[1].trim());
                }
            }
        }
        reader.close();

        if (dbpath == null || pagesize == null || dm_maxfilecount == null || dm_maxpageperfile == null ) {
            throw new Exception("Paramètres manquants dans le fichier de configuration : " + fichierConfig);
        }

        DBConfig config = new DBConfig(dbpath, pagesize, dm_maxfilecount, dm_maxpageperfile);
        return config;
    }


    @Override
    public String toString() {
        return "DBConfig{" +
                "dbpath='" + dbpath + '\'' +
                ", pagesize=" + pagesize +
                ", dm_maxfilecount=" + dm_maxfilecount +
                ", dm_maxpageperfile=" + dm_maxpageperfile +
                '}';
    }

}
