public class TestDBManager {

    public static void main(String[] args) {

        
            // Configuration minimale
            DBConfig config = new DBConfig(
                "./DB",
                4096,
                4,
                2,
                "LRU"
            );

            DBManager manager = new DBManager(config);

            System.out.println("===== TEST DBMANAGER =====\n");

            // Création table 1
            Relation r1 = new Relation(
                "Student",
                java.util.List.of(
                    new ColumnInfo("Id", "INT"),
                    new ColumnInfo("Name", "CHAR(20)")
                )
            );

            manager.AddTable(r1);

            // Création table 2
            Relation r2 = new Relation(
                "Course",
                java.util.List.of(
                    new ColumnInfo("Code", "VARCHAR(10)"),
                    new ColumnInfo("Label", "CHAR(40)")
                )
            );

            manager.AddTable(r2);

            // Afficher
            System.out.println("\n-- Describe tables --");
            manager.DescribeAllTables();

            // Supprimer une table
            System.out.println("\n-- Drop table Student --");
            manager.RemoveTable("Student");

            // Afficher
            System.out.println("\n-- Describe tables --");
            manager.DescribeAllTables();

            // Sauvegarder état
            System.out.println("\n-- Save state --");
            manager.SaveState();

            System.out.println("\nFIN TEST DBMANAGER");


    }
}
