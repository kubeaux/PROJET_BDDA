import java.io.IOException;

public class TestDiskManager {
    public static void main(String[] args) {
        try {
            DBConfig config = DBConfig.loadDBConfig("config.txt");
            System.out.println("Configuration chargée : " + config);

            DiskManager dm = new DiskManager(config);

            dm.Init();
            System.out.println("DiskManager initialisé.");

            PageId page1 = dm.AllocPage();
            System.out.println("Page allouée : " + page1);

            byte[] buffer = new byte[config.getPageSize()];
            for (int i = 0; i < buffer.length; i++) buffer[i] = (byte) i;
            dm.writePage(page1, buffer);
            System.out.println("Page écrite avec succès.");

            byte[] bufferRead = new byte[config.getPageSize()];
            dm.readPage(page1, bufferRead);
            System.out.println("Page lue avec succès.");
            System.out.println("Premier octet lu : " + bufferRead[0]);
            System.out.println("Dernier octet lu : " + bufferRead[config.getPageSize() - 1]);

            dm.DeallocPage(page1);
            System.out.println("Page désallouée : " + page1);

            PageId page2 = dm.AllocPage();
            System.out.println("Page réallouée : " + page2);

            dm.Finish();
            System.out.println("DiskManager terminé.");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
