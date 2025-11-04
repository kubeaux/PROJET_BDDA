public class TestBufferManager {
    public static void main(String[] args) {
        try {
            // Configuration de test
            DBConfig config = new DBConfig("../DB", 4, 4, 2, "LRU"); // 2 buffers pour test
            DiskManager dm = new DiskManager(config);
            dm.Init();
            BufferManager bm = new BufferManager(config, dm);

            // Allouer et écrire une page
            PageId page1 = dm.AllocPage();
            byte[] data = bm.GetPage(page1);
            data[0] = 42;
            bm.FreePage(page1, true);

            // Lire la même page
            byte[] readData = bm.GetPage(page1);
            System.out.println("Premier octet lu : " + readData[0]); // doit être 42
            bm.FreePage(page1, false);

            // Test remplacement LRU
            PageId page2 = dm.AllocPage();
            PageId page3 = dm.AllocPage();
            bm.GetPage(page2); // occupe 2e buffer
            bm.GetPage(page3); // déclenche remplacement
            bm.FreePage(page2, false);
            bm.FreePage(page3, false);

            // Flush final
            bm.FlushBuffers();
            dm.Finish();

            System.out.println("Test BufferManager terminé.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
