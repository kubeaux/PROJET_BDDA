package minisgbd;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SGBD : boucle principale + parsing des commandes du TP6
 */
public class SGBD {

    private final DBConfig config;
    private final DiskManager diskManager;
    private final BufferManager bufferManager;
    private final DBManager dbManager;
    private boolean running = false;

    public SGBD(DBConfig cfg) throws Exception {
        this.config = cfg;
        this.diskManager = new DiskManager(cfg);
        this.diskManager.Init();
        this.bufferManager = new BufferManager(cfg, diskManager);
        this.dbManager = new DBManager(cfg);
        try {
            dbManager.LoadState(diskManager, bufferManager);
        } catch (Exception e) {
            System.err.println("Warning: LoadState failed: " + e.getMessage());
        }
    }

    public void Run() {
        running = true;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while (running && (line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                try {
                    processCommand(line);
                } catch (Exception e) {
                    System.err.println("Erreur: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("I/O error in Run: " + e.getMessage());
        }
    }

    private void processCommand(String cmd) throws Exception {
        String up = cmd.toUpperCase(Locale.ROOT);
        if (up.startsWith("CREATE TABLE ")) {
            ProcessCreateTableCommand(cmd);
        } else if (up.startsWith("DROP TABLES")) {
            ProcessDropTablesCommand(cmd);
        } else if (up.startsWith("DROP TABLE ")) {
            ProcessDropTableCommand(cmd);
        } else if (up.startsWith("DESCRIBE TABLES")) {
            ProcessDescribeTablesCommand(cmd);
        } else if (up.startsWith("DESCRIBE TABLE ")) {
            ProcessDescribeTableCommand(cmd);
        } else if (up.equals("EXIT")) {
            ProcessExitCommand();
        } else {
            System.out.println("Commande inconnue : " + cmd);
        }
    }

    // CREATE TABLE Name (col:TYPE,col:TYPE(...),...)
    public void ProcessCreateTableCommand(String cmd) throws Exception {
        Pattern p = Pattern.compile("^CREATE\\s+TABLE\\s+([A-Za-z0-9_]+)\\s*\\((.*)\\)\\s*$", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(cmd);
        if (!m.find()) throw new IllegalArgumentException("CREATE TABLE : syntaxe invalide");
        String tableName = m.group(1);
        String colsText = m.group(2).trim();

        String[] colTokens = colsText.split(",");

        Relation rel = new Relation(tableName, diskManager, bufferManager, config);

        for (String tok : colTokens) {
            tok = tok.trim();
            int colon = tok.indexOf(':');
            if (colon < 0) throw new IllegalArgumentException("Colonne syntaxe invalide: " + tok);
            String colName = tok.substring(0, colon);
            String typePart = tok.substring(colon + 1).toUpperCase(Locale.ROOT);

            if (typePart.startsWith("INT")) {
                rel.addColumn(new ColumnInfo(colName, ColumnType.INT));
            } else if (typePart.startsWith("FLOAT")) {
                rel.addColumn(new ColumnInfo(colName, ColumnType.FLOAT));
            } else if (typePart.startsWith("CHAR")) {
                int size = extractSize(typePart);
                rel.addColumn(new ColumnInfo(colName, ColumnType.CHAR, size));
            } else if (typePart.startsWith("VARCHAR")) {
                int size = extractSize(typePart);
                rel.addColumn(new ColumnInfo(colName, ColumnType.VARCHAR, size));
            } else {
                throw new IllegalArgumentException("Type colonne inconnu: " + typePart);
            }
        }

        rel.computeRecordSizeAndSlots(config.getPagesize());
        dbManager.AddTable(rel);

        System.out.println("Table créée: " + tableName);
    }

    private int extractSize(String typePart) {
        int p1 = typePart.indexOf('(');
        int p2 = typePart.indexOf(')');
        if (p1 < 0 || p2 < 0) throw new IllegalArgumentException("Taille manquante: " + typePart);
        String n = typePart.substring(p1 + 1, p2).trim();
        return Integer.parseInt(n);
    }

    public void ProcessDropTableCommand(String cmd) throws Exception {
        Pattern p = Pattern.compile("^DROP\\s+TABLE\\s+([A-Za-z0-9_]+)\\s*$", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(cmd);
        if (!m.find()) throw new IllegalArgumentException("DROP TABLE : syntaxe invalide");
        String name = m.group(1);

        Relation rel = dbManager.GetTable(name);
        if (rel == null) {
            System.out.println("Table inexistante: " + name);
            return;
        }

        // désallouer pages (toutes les data pages)
        List<PageId> pages = rel.getDataPages();
        for (PageId pid : pages) diskManager.DeallocPage(pid);

        dbManager.RemoveTable(name);
        System.out.println("Table supprimée: " + name);
    }

    public void ProcessDropTablesCommand(String cmd) throws Exception {
        List<String> names = dbManager.listTableNames();
        for (String name : names) {
            Relation rel = dbManager.GetTable(name);
            if (rel != null) {
                List<PageId> pages = rel.getDataPages();
                for (PageId pid : pages) diskManager.DeallocPage(pid);
            }
            dbManager.RemoveTable(name);
        }
        System.out.println("Toutes les tables supprimées.");
    }

    public void ProcessDescribeTableCommand(String cmd) {
        Pattern p = Pattern.compile("^DESCRIBE\\s+TABLE\\s+([A-Za-z0-9_]+)\\s*$", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(cmd);
        if (!m.find()) { System.out.println("DESCRIBE TABLE : syntaxe invalide"); return; }
        String name = m.group(1);
        dbManager.DescribeTable(name);
    }

    public void ProcessDescribeTablesCommand(String cmd) {
        dbManager.DescribeAllTables();
    }

    public void ProcessExitCommand() {
        try {
            dbManager.SaveState();
        } catch (Exception e) {
            System.err.println("Erreur SaveState: " + e.getMessage());
        }
        try {
            bufferManager.FlushBuffers();
        } catch (Exception e) {
            // ignore
        }
        try {
            diskManager.Finish();
        } catch (Exception e) {
            // ignore
        }
        System.out.println("Bye.");
        running = false;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java minisgbd.SGBD <config-file>");
            System.exit(1);
        }
        DBConfig cfg = DBConfig.loadDBConfig(args[0]);
        SGBD s = new SGBD(cfg);
        s.Run();
    }
}
