import java.nio.ByteBuffer;
import java.util.Arrays;

public class TestRelation {
    public static void main(String[] args) {
        ColumnInfo col1 = new ColumnInfo("id", "INT");
        ColumnInfo col2 = new ColumnInfo("name", "CHAR(5)");
        ColumnInfo col3 = new ColumnInfo("note", "FLOAT");

        Relation rel = new Relation("Etudiants", Arrays.asList(col1, col2, col3));

        Record r1 = new Record(Arrays.asList("1", "Alice", "15.5"));

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        rel.writeRecordToBuffer(r1, buffer, 0);

        Record r2 = new Record();
        rel.readFromBuffer(r2, buffer, 0);

        System.out.println("Record lu : " + r2);

        // Test CHAR padding
        Record r3 = new Record(Arrays.asList("2", "Bob", "12.0"));
        rel.writeRecordToBuffer(r3, buffer, 20);
        Record r4 = new Record();
        rel.readFromBuffer(r4, buffer, 20);
        System.out.println("Record lu : " + r4);
    }
}
