import java.io.*;
import java.util.*;

/**
 * Gestionnaire d'espace disque (Disk Manager)
 * 
 * Inspiré du modèle du livre "Database Management Systems", Ramakrishnan & Gehrke (chapitre 9).
 * Ce module gère la lecture, l'écriture et l'allocation des pages disque.
 */
public class DiskManager {

    private final DBConfig config;
    private final int pageSize;                     // Taille d'une page (en octets)
    private final LinkedList<PageId> freePages;     // Liste chaînée de pages libres
    private final Map<Integer, RandomAccessFile> openFiles; // Cache de fichiers ouverts
    private final File binDataDir;

    public DiskManager(DBConfig config) {
        this.config = config;
        this.pageSize = config.getPagesize(); // on la lit une seule fois ici
        this.freePages = new LinkedList<>();
        this.openFiles = new HashMap<>();
        this.binDataDir = new File(config.getDbpath());
    }

    /**
     * Initialise le gestionnaire disque :
     * - Crée le dossier BinData si inexistant
     * - Recharge la liste des pages libres (si stockée sur disque)
     */
    public void init() {
        if (!binDataDir.exists()) {
            binDataDir.mkdirs();
        }
    }

    /**
     * Alloue une nouvelle page :
     * - Si une page libre existe, on la réutilise.
     * - Sinon, on crée une nouvelle page dans un fichier existant ou nouveau.
     */
    public PageId allocPage() {
        if (!freePages.isEmpty()) {
            return freePages.removeFirst();
        }

        int fileIdx = 0;

        while (true) {
                        // Si on dépasse le nombre maximal de fichiers, plus d'allocation possible
            if (fileIdx >= config.getDmMaxfilecount()) {
                throw new RuntimeException("Plus de place disponible : tous les fichiers BinData sont pleins !");
            }

            File file = new File(binDataDir, "Data" + fileIdx + ".bin");

            try {
                if (!file.exists()) {
                    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                        raf.setLength(pageSize);
                        return new PageId(fileIdx, 0);
                    }
                } else {
                    long fileSize = file.length();
                    int currentPageCount = (int) (fileSize / pageSize);

                    if (currentPageCount < config.getDmMaxpageperfile()) {
                        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                            raf.setLength(fileSize + pageSize);
                            return new PageId(fileIdx, currentPageCount);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }

            fileIdx++;
        }
    }

    /**
     * Lecture d’une page du disque vers un buffer mémoire.
     */
    public void readPage(PageId pageId, byte[] buff) throws IOException {
        if (buff.length < pageSize) {
            throw new IllegalArgumentException("Le buffer doit être au moins de la taille d'une page");
        }

        RandomAccessFile file = getFile(pageId.getFileIdx(), "r");
        long offset = (long) pageId.getPageIdx() * pageSize;
        file.seek(offset);

        int bytesRead = file.read(buff, 0, pageSize);
        if (bytesRead != pageSize) {
            throw new IOException("Lecture incomplète de la page");
        }
    }

    /**
     * Écriture d’une page depuis un buffer vers le disque.
     */
    public void writePage(PageId pageId, byte[] buff) throws IOException {
        if (buff.length < pageSize) {
            throw new IllegalArgumentException("Le buffer doit être au moins de la taille d'une page");
        }

        RandomAccessFile file = getFile(pageId.getFileIdx(), "rw");
        long offset = (long) pageId.getPageIdx() * pageSize;
        file.seek(offset);
        file.write(buff, 0, pageSize);
    }

    /**
     * Marque une page comme libre (désallocation logique).
     */
    public void deallocPage(PageId pageId) {
        freePages.add(pageId);
    }

    /**
     * Ferme proprement tous les fichiers ouverts.
     */
    public void finish() {
        for (RandomAccessFile raf : openFiles.values()) {
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        openFiles.clear();
    }

    /**
     * Récupère un fichier ouvert ou l'ouvre si nécessaire.
     */
    private RandomAccessFile getFile(int fileIdx, String mode) throws IOException {
        if (openFiles.containsKey(fileIdx)) {
            return openFiles.get(fileIdx);
        }
        File file = new File(binDataDir, "Data" + fileIdx + ".bin");
        RandomAccessFile raf = new RandomAccessFile(file, mode);
        openFiles.put(fileIdx, raf);
        return raf;
    }
}
