public class DBConfig {
    private String dbpath;

    public DBConfig(String dbpath) {
        this.dbpath = dbpath;
    }

    public String getDbpath() {
        return dbpath;
    }

    // Méthode statique pour charger à partir d'un fichier texte
    public static DBConfig loadDBConfig(String fichierConfig) throws Exception {
        java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(fichierConfig));
        String line;
        String dbpath = null;

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("dbpath")) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    dbpath = parts[1].trim();
                }
            }
        }
        reader.close();

        if (dbpath == null) {
            throw new Exception("Erreur : dbpath non trouvé dans " + fichierConfig);
        }

        return new DBConfig(dbpath);
    }

    @Override
    public String toString() {
        return "DBConfig{dbpath='" + dbpath + "'}";
    }
}
