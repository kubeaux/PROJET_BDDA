import java.io.*;
import java.util.*;

/**
 * DBManager : gestionnaire des relations (tables) d'une seule base.
 * Utilise un wrapper RelationEntry pour associer Relation + headerPageId.
 */
public class DBManager {
    private DBConfig config;
    private DiskManager diskManager;

    // Wrapper qui stocke la Relation + son headerPageId
    private static class RelationEntry {
        Relation relation;
        PageId headerPageId;

        RelationEntry(Relation r, PageId h) {
            this.relation = r;
            this.headerPageId = h;
        }
    }

    // map: nom table -> RelationEntry
    private Map<String, RelationEntry> relations;

    // fichier de sauvegarde
    private static final String SAVE_FILE_NAME = "database.save";

    public DBManager(DBConfig config) {
        this.config = config;
        this.relations = new LinkedHashMap<>(); // linked pour garder un ordre stable
        this.diskManager = new DiskManager(config);
        // tenter de charger un état existant si présent
        try {
            LoadState();
        } catch (Exception e) {
            // si aucun état ou erreur -> démarrage propre
            // System.err.println("Aucun état chargé (fichier absent ou erreur) : " + e.getMessage());
        }
    }

    /**
     * Ajoute une table. Crée une header page pour la relation et stocke son PageId.
     * @param tab Relation existante
     */
    public void AddTable(Relation tab) {
        String name = tab.getName();
        if (relations.containsKey(name)) {
            // redondance non attendue selon l'énoncé, mais on évite d'écraser
            System.err.println("AddTable: la table '" + name + "' existe déjà, opération ignorée.");
            return;
        }

        try {
            // allouer une page pour le header
            PageId header = diskManager.AllocPage();
            RelationEntry entry = new RelationEntry(tab, header);
            relations.put(name, entry);
            // Optionnel : initialiser le contenu de la header page si besoin
            // Ici on ne met pas de contenu binaire spécifique, mais tu peux le faire si nécessaire.
        } catch (IOException e) {
            throw new RuntimeException("Impossible d'allouer header page pour la table " + name, e);
        }
    }

    /**
     * Retourne l'objet Relation correspondant au nom de table (ou null si absent).
     * @param nomTable nom de la table
     * @return Relation
     */
    public Relation GetTable(String nomTable) {
        RelationEntry entry = relations.get(nomTable);
        return (entry != null) ? entry.relation : null;
    }

    /**
     * Supprime une table : déalloue sa headerPage (au minimum) et l'enlève du catalogue.
     * @param nomTable nom de la table (valide)
     */
    public void RemoveTable(String nomTable) {
        RelationEntry entry = relations.remove(nomTable);
        if (entry == null) {
            // selon l'énoncé, on ne devrait pas tomber ici (suppression d'une table inexistante)
            System.err.println("RemoveTable: la table '" + nomTable + "' n'existe pas.");
            return;
        }
        // au minimum, déallouer la header page
        if (entry.headerPageId != null) {
            diskManager.DeallocPage(entry.headerPageId);
        }
        // Note : si tu veux supprimer toutes les pages appartenant à la relation,
        // il faut garder la liste des pages par relation (non fournie dans le TP actuel).
    }

    /**
     * Supprime toutes les tables : appelle RemoveTable sur chaque table.
     */
    public void RemoveAllTables() {
        // copie la clé pour éviter ConcurrentModification
        List<String> keys = new ArrayList<>(relations.keySet());
        for (String name : keys) {
            RemoveTable(name);
        }
        relations.clear();
    }

    /**
     * Affiche le schéma d'une table au format : Name (C1:TYPE1,C2:TYPE2,...)
     * @param nomTable nom de la table (valide)
     */
    public void DescribeTable(String nomTable) {
        RelationEntry entry = relations.get(nomTable);
        if (entry == null) {
            System.err.println("DescribeTable: table '" + nomTable + "' introuvable.");
            return;
        }
        System.out.println(formatRelationSchema(entry.relation));
    }

    /**
     * Affiche le schéma de toutes les tables, une par ligne, dans l'ordre d'insertion.
     */
    public void DescribeAllTables() {
        for (RelationEntry entry : relations.values()) {
            System.out.println(formatRelationSchema(entry.relation));
        }
    }

    /**
     * Sauvegarde l'état des relations dans dbpath/database.save
     * Format simple :
     * nomTable|fileIdx, pageIdx|C1:TYPE1,C2:TYPE2,...
     */
    public void SaveState() {
        String dbPath = config.getDbPath();
        File dir = new File(dbPath);
        if (!dir.exists()) dir.mkdirs();

        File saveFile = new File(dir, SAVE_FILE_NAME);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(saveFile))) {
            for (Map.Entry<String, RelationEntry> e : relations.entrySet()) {
                String name = e.getKey();
                RelationEntry entry = e.getValue();
                StringBuilder sb = new StringBuilder();

                // header PageId
                String headerPart = "null";
                if (entry.headerPageId != null) {
                    headerPart = entry.headerPageId.getFileIdx() + "," + entry.headerPageId.getPageIdx();
                }

                // schema part
                Relation rel = entry.relation;
                List<ColumnInfo> cols = rel.getColumns();
                StringJoiner sj = new StringJoiner(",");
                for (ColumnInfo c : cols) {
                    sj.add(c.getName() + ":" + c.getType());
                }

                sb.append(name)
                  .append("|")
                  .append(headerPart)
                  .append("|")
                  .append(sj.toString());

                writer.write(sb.toString());
                writer.newLine();
            }
            writer.flush();
        } catch (IOException ex) {
            throw new RuntimeException("Erreur lors de SaveState: " + ex.getMessage(), ex);
        }
    }

    /**
     * Charge l'état depuis dbpath/database.save (si présent).
     * Reconstruit les Relation + header PageId et les ajoute au catalogue.
     */
    public void LoadState() throws Exception {
        String dbPath = config.getDbPath();
        File saveFile = new File(dbPath, SAVE_FILE_NAME);
        if (!saveFile.exists()) {
            // rien à charger
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(saveFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                // format : name|fileIdx,pageIdx|C1:TYPE1,C2:TYPE2,...
                String[] parts = line.split("\\|", 3);
                if (parts.length < 3) continue;

                String name = parts[0];
                String headerPart = parts[1];
                String schemaPart = parts[2];

                PageId header = null;
                if (!"null".equalsIgnoreCase(headerPart)) {
                    String[] idx = headerPart.split(",");
                    int fileIdx = Integer.parseInt(idx[0].trim());
                    int pageIdx = Integer.parseInt(idx[1].trim());
                    header = new PageId(fileIdx, pageIdx);
                }

                // parser le schema
                List<ColumnInfo> cols = new ArrayList<>();
                if (!schemaPart.isEmpty()) {
                    String[] colDefs = schemaPart.split(",");
                    for (String colDef : colDefs) {
                        String[] p = colDef.split(":", 2);
                        String colName = p[0];
                        String colType = (p.length > 1) ? p[1] : "UNKNOWN";
                        cols.add(new ColumnInfo(colName, colType));
                    }
                }

                Relation rel = new Relation(name, cols);
                RelationEntry entry = new RelationEntry(rel, header);
                relations.put(name, entry);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Erreur lors de LoadState: " + ex.getMessage(), ex);
        }
    }

    // ---- Helpers ----

    // Format : Name (C1:TYPE1,C2:TYPE2,...)
    private String formatRelationSchema(Relation rel) {
        StringBuilder sb = new StringBuilder();
        sb.append(rel.getName()).append(" (");
        List<ColumnInfo> cols = rel.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            ColumnInfo c = cols.get(i);
            sb.append(c.getName()).append(":").append(c.getType());
            if (i < cols.size() - 1) sb.append(",");
        }
        sb.append(")");
        return sb.toString();
    }

    // Pour tests ou introspection : retourne le headerPageId (peut être null)
    public PageId getHeaderPageId(String nomTable) {
        RelationEntry entry = relations.get(nomTable);
        return (entry != null) ? entry.headerPageId : null;
    }
}
