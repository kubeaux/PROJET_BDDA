import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestBufferManager {

    public static void main() {
        try {
            String path = "BinDataBufferTest/";
            cleanup(path);

            DBConfig config = new DBConfig(path, 32, 2, 10, 2, "LRU");
            DiskManager dm = new DiskManager(config);
            dm.init();

            BufferManager bm = new BufferManager(config, dm);

            PageId page1 = dm.allocPage();
            PageId page2 = dm.allocPage();
            PageId page3 = dm.allocPage();

            byte[] pattern1 = new byte[config.getPagesize()];
            byte[] pattern2 = new byte[config.getPagesize()];
            byte[] pattern3 = new byte[config.getPagesize()];
            Arrays.fill(pattern1, (byte) 1);
            Arrays.fill(pattern2, (byte) 2);
            Arrays.fill(pattern3, (byte) 3);

            dm.writePage(page1, pattern1);
            dm.writePage(page2, pattern2);
            dm.writePage(page3, pattern3);

            System.out.println("=== Test BufferManager : LRU ===");
            byte[] buff1 = bm.GetPage(page1);
            System.out.println("Lecture page1 correcte ? " + Arrays.equals(pattern1, buff1));
            buff1[0] = 42;
            bm.FreePage(page1, true);

            byte[] buff2 = bm.GetPage(page2);
            System.out.println("Lecture page2 correcte ? " + Arrays.equals(pattern2, buff2));
            bm.FreePage(page2, false);

            byte[] buff3 = bm.GetPage(page3);
            System.out.println("Lecture page3 correcte ? " + Arrays.equals(pattern3, buff3));
            bm.FreePage(page3, false);

            byte[] diskCheck1 = new byte[config.getPagesize()];
            dm.readPage(page1, diskCheck1);
            System.out.println("Page1 écrite sur disque (attendu 42) ? " + (diskCheck1[0] == 42));

            System.out.println("\n=== Changement vers MRU ===");
            bm.SetCurrentReplacementPolicy("MRU");

            byte[] buff1Again = bm.GetPage(page1);
            buff1Again[1] = 11;
            bm.FreePage(page1, false);

            byte[] buff2Again = bm.GetPage(page2);
            buff2Again[0] = 99;
            bm.FreePage(page2, true);

            byte[] buff3Again = bm.GetPage(page3);
            bm.FreePage(page3, false);

            byte[] buff1Check = bm.GetPage(page1);
            System.out.println("MRU : page1 restée en mémoire ? " + (buff1Check[1] == 11));
            bm.FreePage(page1, false);

            byte[] diskCheck2 = new byte[config.getPagesize()];
            dm.readPage(page2, diskCheck2);
            System.out.println("Page2 écrite après MRU (attendu 99) ? " + (diskCheck2[0] == 99));

            byte[] buff3Mod = bm.GetPage(page3);
            buff3Mod[0] = 77;
            bm.FreePage(page3, true);

            System.out.println("\n=== FlushBuffers ===");
            bm.FlushBuffers();

            byte[] buff3AfterFlush = bm.GetPage(page3);
            System.out.println("Flush : page3 sur disque (attendu 77) ? " + (buff3AfterFlush[0] == 77));
            bm.FreePage(page3, false);

            bm.FlushBuffers();
            dm.finish();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void cleanup(String path) {
        File dir = new File(path);
        if (!dir.exists()) {
            return;
        }
        deleteRecursive(dir);
    }

    private static void deleteRecursive(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursive(child);
                }
            }
        }
        file.delete();
    }
}
