package minisgbd;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DiskManager {

    private final DBConfig config;
    private final File binDataDir;

    // Une liste : pour chaque fichier DataX.bin → un tableau bool indiquant page utilisée / libre
    private final ArrayList<boolean[]> bitmaps = new ArrayList<>();

    public DiskManager(DBConfig config) {
        this.config = config;
        this.binDataDir = new File(config.getDbpath(), "BinData");
    }

    public void Init() throws IOException {
        if (!binDataDir.exists()) binDataDir.mkdirs();

        File[] files = binDataDir.listFiles((d, name) -> name.startsWith("Data") && name.endsWith(".bin"));
        if (files == null) return;

        // Trier par nom pour que Data0, Data1, ... soient dans l'ordre
        java.util.Arrays.sort(files);

        for (File f : files) {
            long fileSize = f.length();
            int pageCount = (int)(fileSize / config.getPagesize());
            boolean[] bitmap = new boolean[pageCount];

            // lire bitmap si existe
            File bitmapFile = new File(binDataDir, f.getName() + ".bitmap");
            if (bitmapFile.exists()) {
                try (FileInputStream fis = new FileInputStream(bitmapFile)) {
                    for (int i = 0; i < pageCount; i++) {
                        int b = fis.read();
                        bitmap[i] = (b != 0); // 0 = libre, 1 = occupée
                    }
                }
            } else {
                // si pas de fichier bitmap, considérer toutes les pages existantes comme occupées
                for (int i = 0; i < pageCount; i++) bitmap[i] = true;
            }

            bitmaps.add(bitmap);
        }
    }

    public void Finish() throws IOException {
        // écrire chaque bitmap dans son fichier DataX.bitmap
        for (int i = 0; i < bitmaps.size(); i++) {
            boolean[] bitmap = bitmaps.get(i);
            File bitmapFile = new File(binDataDir, "Data" + i + ".bin.bitmap");
            try (FileOutputStream fos = new FileOutputStream(bitmapFile)) {
                for (boolean b : bitmap) {
                    fos.write(b ? 1 : 0);
                }
            }
        }
    }

    public PageId AllocPage() throws IOException {
        int pagesize = config.getPagesize();

        // Chercher une page libre dans un fichier existant
        for (int fidx = 0; fidx < bitmaps.size(); fidx++) {
            boolean[] map = bitmaps.get(fidx);
            for (int pidx = 0; pidx < map.length; pidx++) {
                if (!map[pidx]) {
                    map[pidx] = true;
                    return new PageId(fidx, pidx);
                }
            }
        }

        // Sinon, créer ou étendre un fichier
        int fileIdx = bitmaps.size();
        if (fileIdx >= config.getDm_maxfilecount()) {
            throw new IOException("Max file count reached");
        }

        // créer un nouveau fichier
        File f = new File(binDataDir, "Data" + fileIdx + ".bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(0);
        raf.close();

        // nouvelle bitmap
        boolean[] bitmap = new boolean[1];
        bitmap[0] = true;
        bitmaps.add(bitmap);

        // allouer 1 page
        expandFile(fileIdx, 1);

        return new PageId(fileIdx, 0);
    }

    private void expandFile(int fileIdx, int pageCount) throws IOException {
        File f = new File(binDataDir, "Data" + fileIdx + ".bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(raf.length());
        byte[] zeros = new byte[config.getPagesize()];
        for (int i = 0; i < pageCount; i++) raf.write(zeros);
        raf.close();
    }

    public void ReadPage(PageId pid, byte[] buffer) throws IOException {
        if (buffer.length != config.getPagesize())
            throw new IllegalArgumentException("buffer taille != pagesize");

        File f = new File(binDataDir, "Data" + pid.getFileIdx() + ".bin");
        RandomAccessFile raf = new RandomAccessFile(f, "r");

        long offset = (long)pid.getPageIdx() * config.getPagesize();
        raf.seek(offset);
        raf.readFully(buffer);
        raf.close();
    }

    public void WritePage(PageId pid, byte[] buffer) throws IOException {
        if (buffer.length != config.getPagesize())
            throw new IllegalArgumentException("buffer taille != pagesize");

        File f = new File(binDataDir, "Data" + pid.getFileIdx() + ".bin");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");

        long offset = (long)pid.getPageIdx() * config.getPagesize();
        raf.seek(offset);
        raf.write(buffer);
        raf.close();
    }

    public void DeallocPage(PageId pid) {
        boolean[] map = bitmaps.get(pid.getFileIdx());
        map[pid.getPageIdx()] = false;
    }
}
