package minisgbd;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DiskManager {

    private final DBConfig config;
    private final ArrayList<PageId> freePageList = new ArrayList<>();
    private int currentFileIdx = 0;
    private int nextPageIdxInCurrentFile = 0;
    private final String binDataPath;

    // Constructeur
    public DiskManager(DBConfig config) {
        this.config = config;
        this.binDataPath = config.getDbpath() + File.separator + "BinData";
    }

    // Création du dossier BinData
    public void Init() throws IOException {
        File dir = new File(binDataPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    // Vide la liste des pages libres
    public void Finish() {
        freePageList.clear();
    }

    // Allocation d'une nouvelle page
    public PageId AllocPage() throws IOException {
        // Si des pages libres existent, on les réutilise
        if (!freePageList.isEmpty()) {
            return freePageList.remove(freePageList.size() - 1);
        }

        if (currentFileIdx >= config.getDmMaxfilecount()) {
            throw new IOException("Nombre maximal de fichiers atteint.");
        }

        String filePath = binDataPath + File.separator + "Data" + currentFileIdx + ".bin";
        File file = new File(filePath);

        // Création du fichier s'il n'existe pas
        if (!file.exists()) {
            file.createNewFile();
        }

        // Allocation d'une page à la fin du fichier
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        long fileLength = raf.length();
        int newPageIdx = (int) (fileLength / config.getPagesize());
        raf.setLength(fileLength + config.getPagesize());
        raf.close();

        nextPageIdxInCurrentFile = newPageIdx + 1;
        return new PageId(currentFileIdx, newPageIdx);
    }

    // Lecture d'une page
    public void ReadPage(PageId pageId, ByteBuffer buff) throws IOException {
        String filePath = binDataPath + File.separator + "Data" + pageId.getFileIdx() + ".bin";
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        long offset = (long) pageId.getPageIdx() * config.getPagesize();
        raf.seek(offset);
        raf.readFully(buff.array(), 0, config.getPagesize());
        raf.close();
    }

    // Écriture d'une page
    public void WritePage(PageId pageId, ByteBuffer buff) throws IOException {
        String filePath = binDataPath + File.separator + "Data" + pageId.getFileIdx() + ".bin";
        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
        long offset = (long) pageId.getPageIdx() * config.getPagesize();
        raf.seek(offset);
        raf.write(buff.array(), 0, config.getPagesize());
        raf.close();
    }

    // Désallocation d'une page
    public void DeallocPage(PageId pageId) {
        if (!freePageList.contains(pageId)) {
            freePageList.add(pageId);
        }

        // Effacement de la page (écriture de zéros)
        try {
            ByteBuffer empty = ByteBuffer.allocate(config.getPagesize());
            WritePage(pageId, empty);
        } catch (IOException e) {
            System.err.println("Erreur lors de la désallocation de la page : " + e.getMessage());
        }
    }
}
