package minisgbd;

import java.io.*;
import java.util.*;

/**
 * DBManager : gestion des relations présentes dans la base
 *
 * Format de sauvegarde (dbpath/database.save) : une ligne par table :
 * TableName|fileIdx,pageIdx|col1Name:TYPE(size?),col2Name:TYPE,...
 *
 * Exemple :
 * Tab1|0,1|C1:FLOAT,C2:INT
 */
public class DBManager {

    private final DBConfig config;
    private final Map<String, Relation> tables = new LinkedHashMap<>();

    public DBManager(DBConfig cfg) {
        this.config = cfg;
    }

    public synchronized void AddTable(Relation tab) {
        if (tab == null) throw new IllegalArgumentException("Relation null");
        tables.put(tab.getName(), tab);
    }

    public synchronized Relation GetTable(String nomTable) {
        return tables.get(nomTable);
    }

    public synchronized void RemoveTable(String nomTable) {
        tables.remove(nomTable);
    }

    public synchronized void RemoveAllTables() {
        tables.clear();
    }

    public synchronized void DescribeTable(String nomTable) {
        Relation r = tables.get(nomTable);
        if (r == null) {
            System.out.println("Table introuvable : " + nomTable);
            return;
        }
        System.out.println(relationSchemaAsString(r));
    }

    public synchronized void DescribeAllTables() {
        for (Relation r : tables.values()) {
            System.out.println(relationSchemaAsString(r));
        }
    }

    private String relationSchemaAsString(Relation r) {
        StringBuilder sb = new StringBuilder();
        sb.append(r.getName()).append(" (");
        List<ColumnInfo> cols = r.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            ColumnInfo c = cols.get(i);
            sb.append(c.getName()).append(":").append(c.getType().name());
            if (c.getType() == ColumnType.CHAR || c.getType() == ColumnType.VARCHAR) {
                sb.append("(").append(c.getSize()).append(")");
            }
            if (i < cols.size() - 1) sb.append(",");
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Sauvegarde l'état courant (schémas + headerPageId) dans dbpath/database.save
     */
    public synchronized void SaveState() throws IOException {
        File dbdir = new File(config.getDbpath());
        if (!dbdir.exists()) dbdir.mkdirs();
        File save = new File(dbdir, "database.save");

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(save))) {
            for (Relation r : tables.values()) {
                // header page
                String header = "-1,-1";
                try {
                    PageId hid = r.getHeaderPageId(); // nécessite getHeaderPageId()
                    if (hid != null) header = hid.getFileIdx() + "," + hid.getPageIdx();
                } catch (Throwable t) {
                    // si la méthode n'existe pas, on ignore proprement (mais idéalement ajoute getHeaderPageId)
                }

                // colonnes
                StringBuilder cols = new StringBuilder();
                List<ColumnInfo> cl = r.getColumns();
                for (int i = 0; i < cl.size(); i++) {
                    ColumnInfo c = cl.get(i);
                    cols.append(c.getName()).append(":").append(c.getType().name());
                    if (c.getType() == ColumnType.CHAR || c.getType() == ColumnType.VARCHAR) {
                        cols.append("(").append(c.getSize()).append(")");
                    }
                    if (i < cl.size() - 1) cols.append(",");
                }

                String line = r.getName() + "|" + header + "|" + cols.toString();
                bw.write(line);
                bw.newLine();
            }
        }
    }

    /**
     * Charge l'état depuis dbpath/database.save.
     * Pour recréer les Relations on a besoin de DiskManager et BufferManager fournis en argument.
     */
    public synchronized void LoadState(DiskManager dm, BufferManager bm) throws IOException, Exception {
        tables.clear();
        File dbdir = new File(config.getDbpath());
        File save = new File(dbdir, "database.save");
        if (!save.exists()) return;

        try (BufferedReader br = new BufferedReader(new FileReader(save))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\|");
                if (parts.length < 3) continue;

                String name = parts[0];
                String[] headParts = parts[1].split(",");
                int hidFile = Integer.parseInt(headParts[0]);
                int hidPage = Integer.parseInt(headParts[1]);

                String colsPart = parts[2];
                String[] toks = colsPart.split(",");
                List<ColumnInfo> cis = new ArrayList<>();
                for (String tok : toks) {
                    tok = tok.trim();
                    if (tok.isEmpty()) continue;
                    int colon = tok.indexOf(':');
                    String colName = tok.substring(0, colon);
                    String rest = tok.substring(colon + 1);
                    if (rest.contains("(")) {
                        int p = rest.indexOf('(');
                        String typeStr = rest.substring(0, p);
                        int size = Integer.parseInt(rest.substring(p + 1, rest.length() - 1));
                        ColumnType ct = ColumnType.valueOf(typeStr);
                        cis.add(new ColumnInfo(colName, ct, size));
                    } else {
                        ColumnType ct = ColumnType.valueOf(rest);
                        cis.add(new ColumnInfo(colName, ct));
                    }
                }

                // creer relation
                Relation rel = new Relation(name, dm, bm, config);
                for (ColumnInfo ci : cis) rel.addColumn(ci);
                rel.computeRecordSizeAndSlots(config.getPagesize());

                // restaurer header page id si possible
                try {
                    rel.setHeaderPageId(new PageId(hidFile, hidPage)); // nécessite setHeaderPageId
                } catch (Throwable t) {
                    // ignore si setter absent
                }

                tables.put(name, rel);
            }
        } catch (IOException e) {
            throw e;
        }
    }

    /** utile pour tests/itérations */
    public synchronized List<String> listTableNames() {
        return new ArrayList<>(tables.keySet());
    }
}
