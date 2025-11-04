import java.util.*;

public class BufferManager {

    private DBConfig dbConfig;
    private DiskManager diskManager;

    private class Buffer {
        PageId pageId;
        byte[] data;
        int pinCount;
        boolean dirty;

        Buffer(int size) {
            data = new byte[size];
            pinCount = 0;
            dirty = false;
            pageId = null;
        }
    }

    private List<Buffer> buffers;
    private LinkedList<Buffer> lruList; // pour LRU/MRU

    private String currentPolicy;

    public BufferManager(DBConfig dbConfig, DiskManager diskManager) {
        this.dbConfig = dbConfig;
        this.diskManager = diskManager;
        this.currentPolicy = dbConfig.getBmPolicy();
        buffers = new ArrayList<>();
        lruList = new LinkedList<>();
        for (int i = 0; i < dbConfig.getBmBufferCount(); i++) {
            Buffer buf = new Buffer(dbConfig.getPageSize());
            buffers.add(buf);
            lruList.add(buf);
        }
    }

    // Récupère une page dans le buffer, applique LRU/MRU si remplacement nécessaire
    public byte[] GetPage(PageId pageId) throws Exception {
        // Chercher si la page est déjà en buffer
        for (Buffer buf : buffers) {
            if (pageId.equals(buf.pageId)) {
                buf.pinCount++;
                lruList.remove(buf);
                lruList.addLast(buf);
                return buf.data;
            }
        }

        // Page non en buffer → trouver un buffer libre ou appliquer remplacement
        Buffer bufToUse = null;
        for (Buffer buf : buffers) {
            if (buf.pinCount == 0 && buf.pageId == null) {
                bufToUse = buf;
                break;
            }
        }
        if (bufToUse == null) {
            // Choisir le buffer à remplacer selon la politique
            if (currentPolicy.equalsIgnoreCase("LRU")) {
                bufToUse = lruList.removeFirst();
            } else { // MRU
                bufToUse = lruList.removeLast();
            }
            // Si dirty, écrire avant de réutiliser
            if (bufToUse.dirty && bufToUse.pageId != null) {
                diskManager.writePage(bufToUse.pageId, bufToUse.data);
            }
            bufToUse.pageId = null;
            bufToUse.pinCount = 0;
            bufToUse.dirty = false;
        }

        // Charger la page depuis le DiskManager
        diskManager.readPage(pageId, bufToUse.data);
        bufToUse.pageId = pageId;
        bufToUse.pinCount = 1;
        bufToUse.dirty = false;
        lruList.addLast(bufToUse);
        return bufToUse.data;
    }

    // Libère une page, met à jour pinCount et dirty
    public void FreePage(PageId pageId, boolean valdirty) {
        for (Buffer buf : buffers) {
            if (pageId.equals(buf.pageId)) {
                buf.pinCount = Math.max(0, buf.pinCount - 1);
                buf.dirty |= valdirty;
                return;
            }
        }
    }

    // Change la politique de remplacement
    public void SetCurrentReplacementPolicy(String policy) {
        this.currentPolicy = policy;
    }

    // Écrit toutes les pages dirty sur le disque et réinitialise les buffers
    public void FlushBuffers() throws Exception {
        for (Buffer buf : buffers) {
            if (buf.dirty && buf.pageId != null) {
                diskManager.writePage(buf.pageId, buf.data);
            }
            buf.pageId = null;
            buf.pinCount = 0;
            buf.dirty = false;
        }
        lruList.clear();
        lruList.addAll(buffers);
    }
}
