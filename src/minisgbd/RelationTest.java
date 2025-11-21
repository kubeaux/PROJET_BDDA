package minisgbd;

import java.util.Arrays;
import java.util.List;

public class RelationTest {

    public static void main(String[] args) throws Exception {

        System.out.println("===== TEST Relation =====");

        Relation rel = buildRelation();

        testInsert(rel);
        testGetAll(rel);
        testDelete(rel);
        testAddDataPage(rel);
        testGetFreeDataPage(rel);
        testWriteRecordToDataPage(rel);

        System.out.println("===== ALL TESTS OK =====");
    }

    private static Relation buildRelation() throws Exception {
        String name = "TestRel";

        // Créer la config et managers
        DBConfig cfg = new DBConfig("./DB_Data", 4096, 10, 5, "LRU");
        DiskManager dm = new DiskManager(cfg);
        dm.Init();
        BufferManager bm = new BufferManager(cfg, dm);

        // Créer la relation
        Relation rel = new Relation(name, dm, bm, cfg);

        // Ajouter les colonnes
        rel.addColumn(new ColumnInfo("id", ColumnType.INT));
        rel.addColumn(new ColumnInfo("name", ColumnType.CHAR, 4));
        rel.addColumn(new ColumnInfo("val", ColumnType.FLOAT));

        // Calculer recordSize et slotsPerPage
        rel.computeRecordSizeAndSlots(cfg.getPagesize());

        return rel;
    }

    // ------------------------------------------------------
    private static void testInsert(Relation rel) throws Exception {
        System.out.println("TEST: InsertRecord");

        Record r = new Record(Arrays.asList("1", "TEST", "1.0"));
        RecordId rid = rel.InsertRecord(r);

        if (rid == null || rid.getPageId() == null || rid.getSlotIdx() < 0) {
            throw new RuntimeException("ERREUR: InsertRecord a retourné un rid invalide !");
        }

        System.out.println("  OK → " + rid.getPageId() + " slot=" + rid.getSlotIdx());
    }

    // ------------------------------------------------------
    private static void testGetAll(Relation rel) throws Exception {
        System.out.println("TEST: GetAllRecords");

        rel.InsertRecord(new Record(Arrays.asList("2", "AAAA", "1.1")));
        rel.InsertRecord(new Record(Arrays.asList("3", "BBBB", "2.2")));

        List<Record> list = rel.GetAllRecords();

        if (list.size() < 3) { // déjà 1 inséré avant
            throw new RuntimeException("ERREUR: GetAllRecords ne retourne pas assez de records !");
        }

        System.out.println("  OK → count=" + list.size());
    }

    // ------------------------------------------------------
    private static void testDelete(Relation rel) throws Exception {
        System.out.println("TEST: DeleteRecord");

        Record r = new Record(Arrays.asList("9", "ZZZZ", "9.9"));
        RecordId rid = rel.InsertRecord(r);

        rel.DeleteRecord(rid);

        List<Record> list = rel.GetAllRecords();
        for (Record rec : list) {
            if (rec.getValues().get(0).equals("9")) {
                throw new RuntimeException("ERREUR: DeleteRecord n'a pas supprimé le record !");
            }
        }

        System.out.println("  OK");
    }

    // ------------------------------------------------------
    private static void testAddDataPage(Relation rel) throws Exception {
        System.out.println("TEST: addDataPage");

        List<PageId> before = rel.getDataPages();

        rel.addDataPage(rel.getBufferManager().getConfig().getPagesize());

        List<PageId> after = rel.getDataPages();

        if (after.size() != before.size() + 1) {
            throw new RuntimeException("ERREUR: addDataPage n'a pas ajouté de page !");
        }

        System.out.println("  OK");
    }

    // ------------------------------------------------------
    private static void testGetFreeDataPage(Relation rel) throws Exception {
        System.out.println("TEST: getFreeDataPageId");

        rel.addDataPage(rel.getBufferManager().getConfig().getPagesize());
        PageId pid = rel.getFreeDataPageId();

        if (pid == null) {
            throw new RuntimeException("ERREUR: getFreeDataPageId retourne null alors qu'il existe une page libre !");
        }

        System.out.println("  OK → " + pid);
    }

    // ------------------------------------------------------
    private static void testWriteRecordToDataPage(Relation rel) throws Exception {
        System.out.println("TEST: writeRecordToDataPage");

        rel.addDataPage(rel.getBufferManager().getConfig().getPagesize());
        PageId pid = rel.getDataPages().get(0);

        Record r = new Record(Arrays.asList("7", "DATA", "4.2"));

        RecordId rid = rel.writeRecordToDataPage(r, pid);

        if (rid == null || !rid.getPageId().equals(pid)) {
            throw new RuntimeException("ERREUR: writeRecordToDataPage a échoué !");
        }

        System.out.println("  OK → " + rid.getPageId() + " slot=" + rid.getSlotIdx());
    }
}
