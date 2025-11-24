import java.util.*;
import java.io.*;

public class SGBD {

    private DBManager dbManager;
    private boolean running = true;

    public SGBD(DBConfig config) {
        dbManager = new DBManager(config);
    }

    /**
     * Boucle principale : lit les commandes ligne par ligne.
     */
    public void Run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (running) {
                String line = reader.readLine();
                if (line == null) {
                    running = false;
                    break;
                }
                ProcessCommand(line.trim());
            }
        } catch (IOException e) {
            System.err.println("Erreur lecture entrée : " + e.getMessage());
        }
    }

    /**
     * Détection et routage des commandes.
     */
    public void ProcessCommand(String cmd) {
        if (cmd.isEmpty()) return;

        String upper = cmd.toUpperCase(Locale.ROOT);

        if (upper.startsWith("CREATE TABLE")) {
            ProcessCreateCommand(cmd);
        }
        else if (upper.startsWith("DROP TABLES")) {
            ProcessDropAllTables();
        }
        else if (upper.startsWith("DROP TABLE")) {
            ProcessDropTable(cmd);
        }
        else if (upper.startsWith("DESCRIBE TABLES")) {
            ProcessDescribeAllTables();
        }
        else if (upper.startsWith("DESCRIBE TABLE")) {
            ProcessDescribeTable(cmd);
        }
        else if (upper.startsWith("EXIT")) {
            ProcessExitCommand();
        }
        else {
            System.err.println("Commande inconnue : " + cmd);
        }
    }

    // ------------------------------------------------------------
    // CREATE TABLE <nom> (<col:type,...>)
    // ------------------------------------------------------------
    public void ProcessCreateCommand(String text) {
        // Exemple :
        // CREATE TABLE Student (Id INT, Name CHAR(20), Grade FLOAT)

        // 1. Extraire "nom(...)" après CREATE TABLE
        String tmp = text.substring("CREATE TABLE".length()).trim();

        // tmp est par ex : Student (Id INT, Name CHAR(20), Grade FLOAT)
        int idxParen = tmp.indexOf('(');
        String tableName = tmp.substring(0, idxParen).trim();

        // 2. Extraire le contenu entre parenthèses
        int idxEndParen = tmp.lastIndexOf(')');
        String inside = tmp.substring(idxParen + 1, idxEndParen).trim();

        // 3. Découper les colonnes par virgule
        String[] colDefs = inside.split(",");

        List<ColumnInfo> cols = new ArrayList<>();

        for (String colDef : colDefs) {
            colDef = colDef.trim();

            // Format attendu : Nom TYPE
            // Donc split sur espace, max 2 parties
            String[] parts = colDef.split("\\s+", 2);
            String colName = parts[0].trim();
            String colType = parts[1].trim();

            cols.add(new ColumnInfo(colName, colType));
        }

        // 4. Créer la relation
        Relation rel = new Relation(tableName, cols);

        // 5. L'enregistrer dans le DBManager
        dbManager.AddTable(rel);
    }

    // ------------------------------------------------------------
    // DROP TABLE nom
    // ------------------------------------------------------------
    public void ProcessDropTable(String text) {
        // Format : DROP TABLE nom
        String tmp = text.substring("DROP TABLE".length()).trim();
        dbManager.RemoveTable(tmp);
    }

    // ------------------------------------------------------------
    // DROP TABLES
    // ------------------------------------------------------------
    public void ProcessDropAllTables() {
        dbManager.RemoveAllTables();
    }

    // ------------------------------------------------------------
    // DESCRIBE TABLE nom
    // ------------------------------------------------------------
    public void ProcessDescribeTable(String text) {
        // Format : DESCRIBE TABLE nom
        String tmp = text.substring("DESCRIBE TABLE".length()).trim();
        dbManager.DescribeTable(tmp);
    }

    // ------------------------------------------------------------
    // DESCRIBE TABLES
    // ------------------------------------------------------------
    public void ProcessDescribeAllTables() {
        dbManager.DescribeAllTables();
    }

    // ------------------------------------------------------------
    // EXIT
    // ------------------------------------------------------------
    public void ProcessExitCommand() {
        dbManager.SaveState();
        running = false;
    }
}
