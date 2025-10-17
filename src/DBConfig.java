package minisgbd; // Bonnes pratiques : utiliser un package

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Représente la configuration de base du SGBDR.
 */
public class DBConfig {
    
    // Attribut
    private String dbpath;
    private int pagesize;
    private int dm_maxfilecount;


    // F. Constructeur
    public DBConfig(String dbpath,int pagesize,int dm_maxfilecount) {
        if (dbpath == null || dbpath.trim().isEmpty()) {
            throw new IllegalArgumentException("Le chemin dbpath ne peut pas être vide.");
        }
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
    }



    // F. Méthode statique pour le chargement
   public static DBConfig LoadDBConfig(String fichierConfig) throws IOException {
    String loadedDbPath = null;
    // Initialisation des nouveaux paramètres à 0 pour la vérification
    int loadedPagesize = 0;
    int loadedDmMaxfilecount = 0;

    try (BufferedReader reader = new BufferedReader(new FileReader(fichierConfig))) {
        String line;
        while ((line = reader.readLine()) != null) {
            String trimmedLine = line.trim();
            
            // On ignore les lignes vides ou celles qui ne contiennent pas le '=' (par exemple, des commentaires)
            if (trimmedLine.isEmpty() || !trimmedLine.contains("=")) {
                continue;
            }
            
            // Sépare la clé et la valeur
            String[] parts = trimmedLine.split("=", 2);
            if (parts.length != 2) {
                continue;
            }
            
            String key = parts[0].trim();
            String value = parts[1].trim().replaceAll("['\"]", ""); // Nettoyage de la valeur

            // Extraction et conversion des valeurs
            switch (key) {
                case "dbpath":
                    loadedDbPath = value;
                    break;
                case "pagesize":
                    // Convertit la chaîne en entier
                    loadedPagesize = Integer.parseInt(value);
                    break;
                case "dm_maxfilecount":
                    // Convertit la chaîne en entier
                    loadedDmMaxfilecount = Integer.parseInt(value);
                    break;
            }
        }
    } catch (NumberFormatException e) {
        // Gère le cas où pagesize ou dm_maxfilecount ne sont pas des nombres
        throw new IllegalArgumentException("Erreur: Une valeur de configuration (pagesize ou dm_maxfilecount) n'est pas un nombre entier valide.", e);
    }
    
    // Validation finale : vérifie si tous les paramètres ont été trouvés et sont valides
    if (loadedDbPath == null || loadedPagesize <= 0 || loadedDmMaxfilecount <= 0) {
        throw new IllegalArgumentException("Erreur: Des paramètres obligatoires (dbpath, pagesize, ou dm_maxfilecount) sont manquants ou invalides (doivent être > 0) dans le fichier de configuration.");
    }

    // Retourne la nouvelle instance en utilisant le constructeur mis à jour
    return new DBConfig(loadedDbPath, loadedPagesize, loadedDmMaxfilecount);
}


    // Getters
    public String getDbpath() {
        return dbpath;
    }
    public int getPagesize(){
        return pagesize;
    }
    public int getDmMaxfilecount(){
        return dm_maxfilecount;
    }
    
    // Setters
     public void setDbpath(String dbpath) {
         this.dbpath = dbpath; 
     }
    public void setPagesize(int pagesize) {
         this.pagesize = pagesize;
    }
    public void setDmMaxFileCount(int dm_maxfilecount) {
        this.dm_maxfilecount = dm_maxfilecount;
    }

    @Override
    public String toString() {
        return "DBConfig [dbpath=" + dbpath + "]";
    }
}