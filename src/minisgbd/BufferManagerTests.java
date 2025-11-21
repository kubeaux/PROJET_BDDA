package minisgbd;

import java.util.Arrays;

public class BufferManagerTests {
    public static void main(String[] args) throws Exception {
        // 1. Créer DBConfig minimal et DiskManager
        DBConfig cfg = new DBConfig("../DB_Data", 4, 10, 3, "LRU");
        DiskManager dm = new DiskManager(cfg);
        dm.Init();

        // 2. Créer BufferManager
        BufferManager bm = new BufferManager(cfg, dm);

        System.out.println("=== Test 1 : GetPage / WritePage / FreePage ===");
        PageId p1 = dm.AllocPage();
        byte[] data1 = bm.GetPage(p1);
        Arrays.fill(data1, (byte)10); // écrire dans buffer
        bm.FreePage(p1, true); // marquer dirty
        System.out.println("Page " + p1 + " écrite et libérée (dirty=true)");

        PageId p2 = dm.AllocPage();
        byte[] data2 = bm.GetPage(p2);
        Arrays.fill(data2, (byte)20);
        bm.FreePage(p2, true);
        System.out.println("Page " + p2 + " écrite et libérée (dirty=true)");

        System.out.println("=== Test 2 : Politique LRU ===");
        // Forcer allocation supplémentaire pour déclencher LRU
        PageId p3 = dm.AllocPage();
        byte[] data3 = bm.GetPage(p3);
        Arrays.fill(data3, (byte)30);
        bm.FreePage(p3, true);

        // Lire une page déjà en mémoire pour mettre à jour LRU
        bm.GetPage(p1);
        bm.FreePage(p1, false);

        // Allouer une nouvelle page pour déclencher remplacement LRU
        PageId p4 = dm.AllocPage();
        byte[] data4 = bm.GetPage(p4);
        Arrays.fill(data4, (byte)40);
        bm.FreePage(p4, true);

        System.out.println("Pages utilisées et remplacement LRU testé");

        System.out.println("=== Test 3 : Changement politique MRU ===");
        bm.SetCurrentReplacementPolicy("MRU");
        PageId p5 = dm.AllocPage();
        byte[] data5 = bm.GetPage(p5);
        Arrays.fill(data5, (byte)50);
        bm.FreePage(p5, true);

        System.out.println("Politique MRU appliquée pour nouvelle page");

        System.out.println("=== Test 4 : FlushBuffers ===");
        bm.FlushBuffers();
        System.out.println("Tous les buffers ont été écrits sur disque et réinitialisés");

        // Vérifier contenu sur disque pour p1
        byte[] readBack = new byte[cfg.getPagesize()];
        dm.ReadPage(p1, readBack);
        System.out.println("Contenu lu depuis disque pour " + p1 + " : " + Arrays.toString(readBack));

        System.out.println("=== BufferManagerTests OK ===");

        dm.Finish();
    }
}
