package minisgbd;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Relation : gère schéma + heap file basique
 *
 * Layouts :
 *  - Header page (une page) :
 *      offset 0..3   : firstFull.fileIdx (int)
 *      4..7          : firstFull.pageIdx (int)
 *      8..11         : firstNotFull.fileIdx (int)
 *      12..15        : firstNotFull.pageIdx (int)
 *      reste du page : unused
 *
 *  - Data page :
 *      offset 0..3   : prev.fileIdx
 *      4..7          : prev.pageIdx
 *      8..11         : next.fileIdx
 *      12..15        : next.pageIdx
 *      16..19        : numSlots (int)
 *      20..20+numSlots-1 : bytemap (1 byte par slot : 0=free,1=used)
 *      records area starts at offset = 20 + numSlots, each record has fixed recordSize bytes
 *
 * NB : bytemap uses 1 byte per slot (plus simple à manipuler).
 */
public class Relation {

    private final String name;
    private final List<ColumnInfo> columns = new ArrayList<>();
    private int recordSize; // taille fixe en octets d'un record
    private int slotsPerPage; // calculé à partir de pagesize et recordSize

    private final DiskManager diskManager;
    private final BufferManager bufferManager;

    private final List<PageId> dataPages = new ArrayList<>();


    private PageId headerPageId;

    public Relation(String name, DiskManager dm, BufferManager bm, DBConfig cfg) throws Exception {
        if (name == null) throw new IllegalArgumentException("name null");
        this.name = name;
        this.diskManager = dm;
        this.bufferManager = bm;
        this.recordSize = 0;
        this.slotsPerPage = 0;

        // crée header page si besoin
        // Allouer la header page via DiskManager (AllocPage) et l'initialiser via BufferManager
        this.headerPageId = dm.AllocPage();
        initEmptyHeader(cfg.getPagesize());
    }

    public String getName() { return name; }
    public List<ColumnInfo> getColumns() { return columns; }
    public PageId getHeaderPageId() { return headerPageId; }
    public void setHeaderPageId(PageId pid) { this.headerPageId = pid; }


    public void addColumn(ColumnInfo c) {
        columns.add(c);
    }

    public BufferManager getBufferManager(){return bufferManager;}
    public DiskManager getDiskManager(){return diskManager;}

    /**
     * Appelé après ajout des colonnes : calcule recordSize et slotsPerPage
     */
    public void computeRecordSizeAndSlots(int pageSize) {
        int rs = 0;
        for (ColumnInfo col : columns) {
            switch (col.getType()) {
                case INT: rs += 4; break;
                case FLOAT: rs += 4; break;
                case CHAR: rs += col.getSize(); break;
                case VARCHAR: rs += 1 + col.getSize(); break; // 1 byte length + max content + padding
            }
        }
        this.recordSize = rs;

        // header in data page fixed = 20 bytes (prev,next,numSlots) + bytemap (numSlots bytes)
        // we solve for numSlots: pagesize >= 20 + numSlots + numSlots*recordSize  => pagesize >= 20 + numSlots*(1+recordSize)
        // => numSlots <= floor((pagesize - 20)/(1+recordSize))
        if (pageSize <= 20 + (1 + recordSize)) {
            this.slotsPerPage = 0;
        } else {
            this.slotsPerPage = (pageSize - 20) / (1 + recordSize);
        }
    }

    public int getRecordSize() { return recordSize; }
    public int getSlotsPerPage() { return slotsPerPage; }

    // ============================
    // Header helpers
    // ============================
    private void initEmptyHeader(int pageSize) throws Exception {
        // write an "empty" header: both firstFull and firstNotFull set to (-1,-1)
        byte[] buff = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(0);
        writePageIdToBuffer(bb, new PageId(-1, -1)); // firstFull
        writePageIdToBuffer(bb, new PageId(-1, -1)); // firstNotFull
        bufferManager.FreePage(headerPageId, true);
    }

    private PageId readFirstFullFromHeader() throws Exception {
        byte[] buff = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(0);
        PageId p = readPageIdFromBuffer(bb);
        bufferManager.FreePage(headerPageId, false);
        return p;
    }

    private PageId readFirstNotFullFromHeader() throws Exception {
        byte[] buff = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(8);
        PageId p = readPageIdFromBuffer(bb);
        bufferManager.FreePage(headerPageId, false);
        return p;
    }

    private void writeFirstFullInHeader(PageId p) throws Exception {
        byte[] buff = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(0);
        writePageIdToBuffer(bb, p);
        bufferManager.FreePage(headerPageId, true);
    }

    private void writeFirstNotFullInHeader(PageId p) throws Exception {
        byte[] buff = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(8);
        writePageIdToBuffer(bb, p);
        bufferManager.FreePage(headerPageId, true);
    }

    // ============================
    // PageId serialization helpers (use 2 ints)
    // ============================
    private void writePageIdToBuffer(ByteBuffer bb, PageId pid) {
        if (pid == null) pid = new PageId(-1, -1);
        bb.putInt(pid.getFileIdx());
        bb.putInt(pid.getPageIdx());
    }

    private PageId readPageIdFromBuffer(ByteBuffer bb) {
        int f = bb.getInt();
        int p = bb.getInt();
        return new PageId(f, p);
    }

    // ============================
    // Data page helpers
    // ============================

    /**
     * Format a freshly allocated data page (zeroed) with header and bytemap set to 0.
     * Also updates the "firstNotFull" header list.
     */
    public void addDataPage(int pageSize) throws Exception {
        // Vérifie que recordSize est calculé
        if (recordSize == 0) throw new IllegalStateException("computeRecordSizeAndSlots not called");

        // 1. Alloue une nouvelle page
        PageId newPid = diskManager.AllocPage();

        // 2. Prépare le contenu de la page
        byte[] buff = bufferManager.GetPage(newPid);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(0);

        // Prev, next = (-1,-1)
        writePageIdToBuffer(bb, new PageId(-1, -1));
        bb.position(8);
        writePageIdToBuffer(bb, new PageId(-1, -1));
        bb.position(16);

        // Slots par page
        bb.putInt(slotsPerPage);

        // Bitmap : tous les slots libres (0)
        for (int i = 0; i < slotsPerPage; i++) bb.put((byte)0);

        // Enregistre la page
        bufferManager.FreePage(newPid, true);

        // 3. Insère la page dans la liste "not full"
        PageId firstNotFull = readFirstNotFullFromHeader();

        // La nouvelle page.next = ancienne tête (ou -1 si aucune)
        buff = bufferManager.GetPage(newPid);
        bb = ByteBuffer.wrap(buff);
        bb.position(8); // offset "next"
        writePageIdToBuffer(bb, firstNotFull);
        bufferManager.FreePage(newPid, true);

        // Si une ancienne tête existe, mettre à jour son prev = newPid
        if (firstNotFull.getFileIdx() != -1) {
            byte[] oldHeadBuff = bufferManager.GetPage(firstNotFull);
            ByteBuffer oldBb = ByteBuffer.wrap(oldHeadBuff);
            oldBb.position(0); // offset "prev"
            writePageIdToBuffer(oldBb, newPid);
            bufferManager.FreePage(firstNotFull, true);
        }

        // 4. Mettre à jour le header pour pointer sur la nouvelle tête
        writeFirstNotFullInHeader(newPid);
        dataPages.add(newPid);  // <-- met à jour la liste interne

    }







    /**
     * Retourne un PageId d'une page qui a assez de place pour sizeRecord octets.
     * Ici on suppose recordSize fixe, donc test suffit de vérifier qu'il y a au moins
     * un slot libre (bytemap contains a zero).
     */
    public PageId getFreeDataPageId() throws Exception {
        PageId cur = readFirstNotFullFromHeader();
        while (cur.getFileIdx() != -1) {
            // inspect bytemap
            byte[] buff = bufferManager.GetPage(cur);
            ByteBuffer bb = ByteBuffer.wrap(buff);
            bb.position(16);
            int numSlots = bb.getInt();
            int bytemapPos = 20;
            boolean found = false;
            for (int i = 0; i < numSlots; i++) {
                byte b = bb.get(bytemapPos + i);
                if (b == 0) { found = true; break; }
            }
            bufferManager.FreePage(cur, false);
            if (found) return cur;
            // move to next in notFull list
            // next is at offset 8
            buff = bufferManager.GetPage(cur);
            bb = ByteBuffer.wrap(buff);
            bb.position(8);
            PageId next = readPageIdFromBuffer(bb);
            bufferManager.FreePage(cur, false);
            cur = next;
        }
        return null;
    }

    /**
     * Ecrit un record dans la page identifiée (on suppose qu'il y a assez de place).
     * Renvoie le RecordId correspondant.
     */
    public RecordId writeRecordToDataPage(Record record, PageId pid) throws Exception {
        byte[] buff = bufferManager.GetPage(pid);
        ByteBuffer bb = ByteBuffer.wrap(buff);

        // lire header
        bb.position(16);
        int numSlots = bb.getInt();
        int bytemapPos = 20;
        int recordAreaPos = 20 + numSlots;

        // chercher premier slot libre
        int freeSlot = -1;
        for (int i = 0; i < numSlots; i++) {
            byte b = bb.get(bytemapPos + i);
            if (b == 0) { freeSlot = i; break; }
        }
        if (freeSlot == -1) {
            bufferManager.FreePage(pid, false);
            throw new IllegalStateException("no free slot on page");
        }

        // écrire le record au bon offset
        int recOffset = recordAreaPos + freeSlot * recordSize;
        // use TP4 writeRecordToBuffer logic inline (to avoid circular dependency)
        writeRecordBytesToByteBuffer(bb, recOffset, record);

        // set bytemap
        bb.put(bytemapPos + freeSlot, (byte)1);

        // check if page now full -> if yes update header lists
        boolean becameFull = true;
        for (int i = 0; i < numSlots; i++) {
            if (bb.get(bytemapPos + i) == 0) { becameFull = false; break; }
        }

        bufferManager.FreePage(pid, true);

        if (becameFull) {
            // remove pid from notFull list and add to full list head
            removeFromNotFullList(pid);
            addToFullListHead(pid);
        }

        return new RecordId(pid, freeSlot);
    }

    /**
     * Lit et retourne tous les records présents dans la page pageId
     */
    public List<Record> getRecordsInDataPage(PageId pid) throws Exception {
        List<Record> res = new ArrayList<>();
        byte[] buff = bufferManager.GetPage(pid);
        ByteBuffer bb = ByteBuffer.wrap(buff);

        bb.position(16);
        int numSlots = bb.getInt();
        int bytemapPos = 20;
        int recordAreaPos = 20 + numSlots;

        for (int i = 0; i < numSlots; i++) {
            byte b = bb.get(bytemapPos + i);
            if (b == 1) {
                // read record at offset
                int recOffset = recordAreaPos + i * recordSize;
                Record r = new Record();
                readRecordBytesFromByteBuffer(bb, recOffset, r);
                res.add(r);
            }
        }

        bufferManager.FreePage(pid, false);
        return res;
    }

    /**
     * Retourne la liste de PageId de toutes les pages de donnée (en parcourant la lists full + notFull)
     */
    public List<PageId> getDataPages() {
        return new ArrayList<>(dataPages);
    }




    // ============================
    // High-level API : Insert / GetAll / Delete
    // ============================

    public RecordId InsertRecord(Record record) throws Exception {
        // ensure sizes computed
        if (recordSize == 0) throw new IllegalStateException("call computeRecordSizeAndSlots first");

        // find a page with free slot
        PageId pid = getFreeDataPageId();
        if (pid == null) {
            // add data page and retry
            addDataPage(bufferManager.getConfig().getPagesize()); // but config is private -> use bufferManager's config? workaround: recompute earlier
            // to avoid coupling, assume computeRecordSizeAndSlots called with pagesize earlier and we stored slotsPerPage
            // find again
            pid = getFreeDataPageId();
            if (pid == null) throw new IllegalStateException("could not create a new data page");
        }
        return writeRecordToDataPage(record, pid);
    }

    public List<Record> GetAllRecords() throws Exception {
        List<Record> res = new ArrayList<>();
        List<PageId> pages = getDataPages();
        for (PageId pid : pages) {
            List<Record> pageRecords = getRecordsInDataPage(pid);
            res.addAll(pageRecords);
        }
        return res;
    }

    public void DeleteRecord(RecordId rid) throws Exception {
        PageId pid = rid.getPageId();
        int slot = rid.getSlotIdx();

        byte[] buff = bufferManager.GetPage(pid);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(16);
        int numSlots = bb.getInt();
        int bytemapPos = 20;
        if (slot < 0 || slot >= numSlots) {
            bufferManager.FreePage(pid, false);
            throw new IllegalArgumentException("slot out of range");
        }
        // mark slot free
        bb.put(bytemapPos + slot, (byte)0);
        bufferManager.FreePage(pid, true);

        // If page becomes empty -> remove from lists and dealloc page
        // check emptiness
        buff = bufferManager.GetPage(pid);
        bb = ByteBuffer.wrap(buff);
        bb.position(16);
        numSlots = bb.getInt();
        bytemapPos = 20;
        boolean any = false;
        for (int i = 0; i < numSlots; i++) {
            if (bb.get(bytemapPos + i) == 1) { any = true; break; }
        }
        bufferManager.FreePage(pid, false);

        if (!any) {
            // remove from lists
            removeFromFullOrNotFullLists(pid);
            // dealloc page via DiskManager
            diskManager.DeallocPage(pid);
        }
    }

    // ============================
    // Helpers for record serialization (reuse TP4 logic)
    // ============================

    private void writeRecordBytesToByteBuffer(ByteBuffer bb, int recOffset, Record record) {
        bb.position(recOffset);
        List<String> values = record.getValues();
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            String val = values.get(i);
            switch (col.getType()) {
                case INT:
                    bb.putInt(Integer.parseInt(val));
                    break;
                case FLOAT:
                    bb.putFloat(Float.parseFloat(val));
                    break;
                case CHAR:
                    byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
                    if (bytes.length > col.getSize()) throw new RuntimeException("CHAR too long");
                    bb.put(bytes);
                    for (int k = bytes.length; k < col.getSize(); k++) bb.put((byte)0);
                    break;
                case VARCHAR:
                    byte[] b2 = val.getBytes(StandardCharsets.UTF_8);
                    if (b2.length > col.getSize()) throw new RuntimeException("VARCHAR too long");
                    bb.put((byte)b2.length);
                    bb.put(b2);
                    for (int k = b2.length; k < col.getSize(); k++) bb.put((byte)0);
                    break;
            }
        }
    }

    private void readRecordBytesFromByteBuffer(ByteBuffer bb, int recOffset, Record r) {
        bb.position(recOffset);
        for (ColumnInfo col : columns) {
            switch (col.getType()) {
                case INT:
                    r.addValue(String.valueOf(bb.getInt()));
                    break;
                case FLOAT:
                    r.addValue(String.valueOf(bb.getFloat()));
                    break;
                case CHAR:
                    byte[] c = new byte[col.getSize()];
                    bb.get(c);
                    r.addValue(new String(c, StandardCharsets.UTF_8).trim());
                    break;
                case VARCHAR:
                    int len = bb.get() & 0xFF;
                    byte[] v = new byte[len];
                    bb.get(v);
                    // skip padding
                    for (int skip = len; skip < col.getSize(); skip++) bb.get();
                    r.addValue(new String(v, StandardCharsets.UTF_8));
                    break;
            }
        }
    }

    // ============================
    // List management helpers (doubly linked lists via page header prev/next)
    // ============================

    private void removeFromNotFullList(PageId pid) throws Exception {
        // read page -> get prev and next
        byte[] buff = bufferManager.GetPage(pid);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(0);
        PageId prev = readPageIdFromBuffer(bb);
        bb.position(8);
        PageId next = readPageIdFromBuffer(bb);
        bufferManager.FreePage(pid, false);

        // fix prev.next = next
        if (prev.getFileIdx() != -1) {
            byte[] pb = bufferManager.GetPage(prev);
            ByteBuffer pbb = ByteBuffer.wrap(pb);
            pbb.position(8);
            writePageIdToBuffer(pbb, next);
            bufferManager.FreePage(prev, true);
        } else {
            // pid was head of notFull list
            writeFirstNotFullInHeader(next);
        }

        // fix next.prev = prev
        if (next.getFileIdx() != -1) {
            byte[] nb = bufferManager.GetPage(next);
            ByteBuffer nbb = ByteBuffer.wrap(nb);
            nbb.position(0);
            writePageIdToBuffer(nbb, prev);
            bufferManager.FreePage(next, true);
        }
    }

    private void addToFullListHead(PageId pid) throws Exception {
        PageId head = readFirstFullFromHeader();
        // set pid.next = head
        byte[] pb = bufferManager.GetPage(pid);
        ByteBuffer pbb = ByteBuffer.wrap(pb);
        pbb.position(8);
        writePageIdToBuffer(pbb, head);
        // prev = (-1,-1)
        pbb.position(0);
        writePageIdToBuffer(pbb, new PageId(-1, -1));
        bufferManager.FreePage(pid, true);

        if (head.getFileIdx() != -1) {
            byte[] hb = bufferManager.GetPage(head);
            ByteBuffer hbb = ByteBuffer.wrap(hb);
            hbb.position(0);
            writePageIdToBuffer(hbb, pid);
            bufferManager.FreePage(head, true);
        }
        writeFirstFullInHeader(pid);
    }

    private void removeFromFullOrNotFullLists(PageId pid) throws Exception {
        // generic removal: same as removeFromNotFullList but also check header heads
        byte[] buff = bufferManager.GetPage(pid);
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.position(0);
        PageId prev = readPageIdFromBuffer(bb);
        bb.position(8);
        PageId next = readPageIdFromBuffer(bb);
        bufferManager.FreePage(pid, false);

        if (prev.getFileIdx() != -1) {
            byte[] pb = bufferManager.GetPage(prev);
            ByteBuffer pbb = ByteBuffer.wrap(pb);
            pbb.position(8);
            writePageIdToBuffer(pbb, next);
            bufferManager.FreePage(prev, true);
        } else {
            // could be head of full or notFull -> need to check which list contains pid as head
            PageId headFull = readFirstFullFromHeader();
            if (headFull.getFileIdx() == pid.getFileIdx() && headFull.getPageIdx() == pid.getPageIdx()) {
                writeFirstFullInHeader(next);
            } else {
                PageId headNotFull = readFirstNotFullFromHeader();
                if (headNotFull.getFileIdx() == pid.getFileIdx() && headNotFull.getPageIdx() == pid.getPageIdx()) {
                    writeFirstNotFullInHeader(next);
                }
            }
        }
        if (next.getFileIdx() != -1) {
            byte[] nb = bufferManager.GetPage(next);
            ByteBuffer nbb = ByteBuffer.wrap(nb);
            nbb.position(0);
            writePageIdToBuffer(nbb, prev);
            bufferManager.FreePage(next, true);
        }
        // finally clear pid prev/next
        buff = bufferManager.GetPage(pid);
        bb = ByteBuffer.wrap(buff);
        bb.position(0);
        writePageIdToBuffer(bb, new PageId(-1, -1));
        bb.position(8);
        writePageIdToBuffer(bb, new PageId(-1, -1));
        bufferManager.FreePage(pid, true);
    }
}
