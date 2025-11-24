import java.lang.reflect.Method;

public class TestSGBD {

    public static void main(String[] args) {

        try {
            DBConfig config = DBConfig.loadDBConfig("config.txt");

            SGBD sgbd = new SGBD(config);

            System.out.println("===== TEST SGBD =====\n");

            // Accès à ProcessCommand via réflexion car il est public mais plus simple que Run()
            Method process = SGBD.class.getMethod("ProcessCommand", String.class);

            process.invoke(sgbd, "CREATE TABLE Student (Id INT, Name CHAR(20), Grade FLOAT)");
            process.invoke(sgbd, "CREATE TABLE Course (Code VARCHAR(10), Label CHAR(40))");
            process.invoke(sgbd, "DESCRIBE TABLES");
            process.invoke(sgbd, "DESCRIBE TABLE Student");
            process.invoke(sgbd, "DROP TABLE Student");
            process.invoke(sgbd, "DESCRIBE TABLES");
            process.invoke(sgbd, "EXIT");

            System.out.println("\nFIN TEST SGBD");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
