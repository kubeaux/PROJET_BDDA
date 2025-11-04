import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

public class DiskManager {
    private DBConfig dbConfig;
    private List<PageId> freePages;

    public DiskManager(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
        this.freePages = new ArrayList<>();
    }

    public PageId AllocPage() throws IOException {
        // Si une page libre est dispo, on la réutilise
        if (!freePages.isEmpty()) {
            return freePages.remove(0);
        }

        // Sinon on crée une nouvelle page dans le fichier
        int fileIdx = 0;
        String fileName;
        RandomAccessFile file;

        // Trouver le premier fichier disponible ou créer un nouveau
        while (true) {
            fileName = dbConfig.getDbPath() + "/Data" + fileIdx + ".bin";
            try {
                file = new RandomAccessFile(fileName, "rw");
                break;
            } catch (IOException e) {
                // Si le fichier n'existe pas, on le crée
                file = new RandomAccessFile(fileName, "rw");
                break;
            }
        }

        long fileLength = file.length();
        int pageIdx = (int) (fileLength / dbConfig.getPageSize());
        file.setLength(fileLength + dbConfig.getPageSize());
        file.close();

        return new PageId(fileIdx, pageIdx);
    }

    public void readPage(PageId pageId, byte[] buffer) throws IOException {
        String fileName = dbConfig.getDbPath() + "/Data" + pageId.getFileIdx() + ".bin";
        RandomAccessFile file = new RandomAccessFile(fileName, "r");
        file.seek((long) pageId.getPageIdx() * dbConfig.getPageSize());
        file.read(buffer);
        file.close();
    }

    public void writePage(PageId pageId, byte[] buffer) throws IOException {
        String fileName = dbConfig.getDbPath() + "/Data" + pageId.getFileIdx() + ".bin";
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        file.seek((long) pageId.getPageIdx() * dbConfig.getPageSize());
        file.write(buffer);
        file.close();
    }

    public void DeallocPage(PageId pageId) {
        freePages.add(pageId);
    }

    public void Init() {
        freePages.clear();
    }

    public void Finish() {
        freePages.clear();
    }
}
