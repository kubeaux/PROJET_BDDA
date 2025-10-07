import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestDiskManager {

    public static void main() {
        try {
            // Config : petit test
            DBConfig config = new DBConfig("BinData/", 64, 4, 8); 
            // 4 fichiers max, 8 pages par fichier
            DiskManager dm = new DiskManager(config);
            dm.init();

            List<PageId> allPages = new ArrayList<>();

            System.out.println("=== Allocation des pages ===");
            try {
                while (true) {
                    PageId page = dm.allocPage();
                    allPages.add(page);
                    System.out.println("Alloué : " + page);
                }
            } catch (Exception e) {
                System.out.println("Fin allocation : " + e.getMessage());
            }

            System.out.println("\n=== Écriture et lecture des pages ===");
            byte[] buffer = new byte[config.getPagesize()];
            for (int i = 0; i < buffer.length; i++) buffer[i] = (byte) (i % 256);

            for (PageId page : allPages) {
                dm.writePage(page, buffer);
                byte[] readBuffer = new byte[config.getPagesize()];
                dm.readPage(page, readBuffer);

                boolean ok = true;
                for (int i = 0; i < buffer.length; i++) {
                    if (buffer[i] != readBuffer[i]) {
                        ok = false;
                        break;
                    }
                }
                System.out.println(page + " contenu correct ? " + ok);
            }

            System.out.println("\n=== Désallocation de quelques pages ===");
            for (int i = 0; i < allPages.size(); i += 3) { // désalloue une page sur 3
                dm.deallocPage(allPages.get(i));
                System.out.println("Désalloué : " + allPages.get(i));
            }

            System.out.println("\n=== Réallocation des pages libérées ===");
            List<PageId> newPages = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                PageId page = dm.allocPage();
                newPages.add(page);
                System.out.println("Réalloué : " + page);
            }

            dm.finish();
            System.out.println("\nTest DiskManager terminé.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
