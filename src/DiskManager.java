import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


public class DiskManager {
    private DBConfig config;
    private LinkedList<PageId> freePages;
    private Map<Integer, RandomAccessFile> openFiles;
    
    private RandomAccessFile raf;

    public DiskManager(DBConfig config) {
        this.config = config;
        this.freePages = new LinkedList<>();
        this.openFiles = new HashMap<>();
    }

    public PageId AllocPage(){

        // 1 - Réutiliser une page libre si elle est disponible

        if (!freePages.isEmpty()) {
            return freePages.removeFirst();
        }

        // 2 - Sinon, on doit allouer une nouvelle page

        byte pagesize = config.getPagesize();
        int maxPagesPerFile = config.getDmMaxfilecount();

        int fileIdx = 0;
        File binDataDir = new File("BinData");

        while (true) {

            File file = new File(binDataDir, "Data" + fileIdx + ".bin");

            if (!file.exists()) {
                try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                    raf.setLength(pagesize);
                    return new PageId(fileIdx, 0);
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            } else {

                long fileSize = file.length();
                int currentPageCount = (int) (fileSize / pagesize);

                if (currentPageCount < maxPagesPerFile) {
                    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                        raf.setLength(fileSize);
                        return new PageId(fileIdx, currentPageCount);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }

            fileIdx++;
        }
    }

    public void ReadPage(PageId pageId, byte[] buff) throws IOException {
        if (buff.length < pagesize) {
            throw new IllegalArgumentException("Le buffer doit être au moins de la taille d'une page");   
        }

        long offset = pageId.getFileIdx() * maxPagesPerFile * pagesize + pageId.getPageIdx() * pagesize;
        raf.seek(offset);
        int bytesRead = raf.read(buff, 0, pagesize);

        if (bytesRead != pagesize) {
            throw new IOException("Lecture incomplète de la page");
        }
    }
    
    public void Init(){

    }
}
