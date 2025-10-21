package minisgbd;
import java.nio.ByteBuffer;

public class TestRelation {
    public static void main(String[] args) {
        // Création d’une relation simple
        Relation rel = new Relation("Etudiants");
        rel.addColumn("id", "INT");
        rel.addColumn("note", "FLOAT");
        rel.addColumn("nom", "CHAR(10)");
        rel.addColumn("description", "VARCHAR(20)");

        // Création d’un record
        Record rec = new Record();
        rec.addValue("123");
        rec.addValue("17.5");
        rec.addValue("Ali");
        rec.addValue("Etudiant exemplaire");

        // Buffer (page mémoire)
        ByteBuffer buffer = ByteBuffer.allocate(256);

        // Écriture du record dans le buffer
        rel.writeRecordToBuffer(rec, buffer, 0);

        // Lecture du record
        Record recLu = new Record();
        rel.readFromBuffer(recLu, buffer, 0);

        // Vérification
        System.out.println("Record original : " + rec);
        System.out.println("Record lu       : " + recLu);
    }
}
