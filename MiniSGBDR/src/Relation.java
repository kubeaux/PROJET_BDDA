import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

public class Relation {
    private String name;
    private List<ColumnInfo> columns;

    private PageId headerPageId;
    private int slotCount;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private int recordSize;
    private int headerPageOffset = 8;

    public Relation(String name, List<ColumnInfo> columns,PageId headerPageId,DiskManager dm,BufferManager bm) {
        this.name = name;
        this.columns = columns;
        this.diskManager = dm;
        this.bufferManager = bm;
        this.headerPageId = headerPageId;

        this.recordSize = 0;
        for (ColumnInfo col : columns) {
            String type = col.getType();
            if (type.equals("INT")) recordSize += 4;
            else if (type.equals("FLOAT")) recordSize += 4;
            else if (type.startsWith("CHAR")) {
                recordSize += Integer.parseInt(type.substring(5, type.length() - 1)) * 2;
            } else if (type.startsWith("VARCHAR")) {
                recordSize += Integer.parseInt(type.substring(8, type.length() - 1)) * 2;
            }
        }

        int pageSize = 4098;
        int headerSize = 20;
        this.slotCount = (pageSize - headerSize) / (1 + recordSize);
    }
    public void addDataPage() throws Exception {
        PageId newPageId = diskManager.AllocPage();
        byte[] pageData = bufferManager.GetPage(newPageId);
        ByteBuffer buffer = ByteBuffer.wrap(pageData);

        byte[] headerData = bufferManager.GetPage(this.headerPageId);
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerData);

        headerBuffer.position(8);
        int oldFreeFileIdx = headerBuffer.getInt();
        int oldFreePageIdx = headerBuffer.getInt();

        headerBuffer.position(8);
        headerBuffer.putInt(newPageId.getFileIdx());
        headerBuffer.putInt(newPageId.getPageIdx());

        buffer.position(0);
        buffer.putInt(oldFreeFileIdx);
        buffer.putInt(oldFreePageIdx);

        buffer.putInt(-1);
        buffer.putInt(-1);

        int bytemapOffset = 20;
        for (int i = 0; i < slotCount; i++) {
            buffer.put(bytemapOffset + i, (byte) 0);
        }

        if (oldFreePageIdx != -1) {
            PageId oldFreeId = new PageId(oldFreeFileIdx, oldFreePageIdx);
            byte[] oldFreeData = bufferManager.GetPage(oldFreeId);
            ByteBuffer oldFreeBuff = ByteBuffer.wrap(oldFreeData);

            oldFreeBuff.position(8);
            oldFreeBuff.putInt(newPageId.getFileIdx());
            oldFreeBuff.putInt(newPageId.getPageIdx());
            bufferManager.FreePage(oldFreeId, true);
        }

        bufferManager.FreePage(this.headerPageId, true);
        bufferManager.FreePage(newPageId, true);
    }
    
    public PageId getFreeDataPageId(int sizeRecord) throws Exception {
        byte[] headerData = bufferManager.GetPage(this.headerPageId);
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerData);

        headerBuffer.position(8);
        int currentFileIdx = headerBuffer.getInt();
        int currentPageIdx = headerBuffer.getInt();

        bufferManager.FreePage(this.headerPageId, false);

        while (currentPageIdx != -1) {
            PageId currentId = new PageId(currentFileIdx, currentPageIdx);
            byte[] pageData = bufferManager.GetPage(currentId);
            ByteBuffer pageBuffer = ByteBuffer.wrap(pageData);

            int bytemapOffset = 20;
            for (int i = 0; i < slotCount; i++) {
                if (pageBuffer.get(bytemapOffset + i) == 0) {
                    bufferManager.FreePage(currentId, false);
                    return currentId;
                }
            }

            pageBuffer.position(0);
            currentFileIdx = pageBuffer.getInt();
            currentPageIdx = pageBuffer.getInt();

            bufferManager.FreePage(currentId, false);
        }

        return null;
    }

    public String getName() {
        return name;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    private int computeRecordSize() {
        int size = 0;
        for (ColumnInfo col : columns) {
            String type = col.getType();
            if (type.equals("INT") || type.equals("FLOAT")) {
                size += 4;
            } else if (type.startsWith("CHAR(")) {
                int n = Integer.parseInt(type.substring(5, type.length() - 1));
                size += n;
            } else if (type.startsWith("VARCHAR(")) {
                int n = Integer.parseInt(type.substring(8, type.length() - 1));
                size += n;
            } else {
                throw new RuntimeException("Type non supporté: " + type);
            }
        }
        return size;
    }

    private void initHeaderPageIfNeeded() throws Exception {
        byte[] header = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(header);

        int fullFile = bb.getInt(HP_FULL_HEAD_OFFSET);
        int fullPage = bb.getInt(HP_FULL_HEAD_OFFSET + 4);
        int freeFile = bb.getInt(HP_FREE_HEAD_OFFSET);
        int freePage = bb.getInt(HP_FREE_HEAD_OFFSET + 4);

        boolean uninitialized = fullFile == 0 && fullPage == 0 && freeFile == 0 && freePage == 0;
        
        if (uninitialized) {
            bb.putInt(HP_FULL_HEAD_OFFSET, INVALID_FILE_IDX);
            bb.putInt(HP_FULL_HEAD_OFFSET + 4, INVALID_PAGE_IDX);
            bb.putInt(HP_FREE_HEAD_OFFSET, INVALID_FILE_IDX);
            bb.putInt(HP_FREE_HEAD_OFFSET + 4, INVALID_PAGE_IDX);
            bufferManager.FreePage(headerPageId, true);
        } else {
            bufferManager.FreePage(headerPageId, false);
        }
        
    }

    private PageId readPageId(byte[] page, int offset) {
        ByteBuffer bb = ByteBuffer.wrap(page);
        int fileIdx = bb.getInt(offset);
        int pageIdx = bb.getInt(offset + 4);
        if (fileIdx == INVALID_FILE_IDX && pageIdx == INVALID_PAGE_IDX) {
            return null;
        }
        return new PageId(fileIdx, pageIdx);
    }

    private void writePageId(byte[] page, int offset, PageId pid) {
        ByteBuffer bb = ByteBuffer.wrap(page);
        if (pid == null) {
            bb.putInt(offset, INVALID_FILE_IDX);
            bb.putInt(offset + 4, INVALID_PAGE_IDX);
        } else {
            bb.putInt(offset, pid.getFileIdx());
            bb.putInt(offset + 4, pid.getPageIdx());
        }
    }

    private void addPageToList(PageId pageId, boolean fullList) throws Exception {
        byte[] header = bufferManager.GetPage(headerPageId);
        int headOffset = fullList ? HP_FULL_HEAD_OFFSET : HP_FREE_HEAD_OFFSET;
        PageId head = readPageId(header, headOffset);

        byte[] page = bufferManager.GetPage(pageId);
        writePageId(page, DP_PREV_PAGE_OFFSET, null);
        writePageId(page, DP_NEXT_PAGE_OFFSET, head);
        bufferManager.FreePage(pageId, true);

        if (head != null) {
            byte[] headPage = bufferManager.GetPage(head);
            writePageId(headPage, DP_PREV_PAGE_OFFSET, pageId);
            bufferManager.FreePage(head, true);
        }

        writePageId(header, headOffset, pageId);
        bufferManager.FreePage(headerPageId, true);
    }

    private void removePageFromList(PageId pageId, boolean fullList) throws Exception {
        byte[] header = bufferManager.GetPage(headerPageId);
        int headOffset = fullList ? HP_FULL_HEAD_OFFSET : HP_FREE_HEAD_OFFSET;
        PageId head = readPageId(header, headOffset);

        byte[] page = bufferManager.GetPage(pageId);
        PageId prev = readPageId(page, DP_PREV_PAGE_OFFSET);
        PageId next = readPageId(page, DP_NEXT_PAGE_OFFSET);

        if (head != null && head.getFileIdx() == pageId.getFileIdx() && head.getPageIdx() == pageId.getPageIdx()) {
            writePageId(header, headOffset, next);
        }

        if (prev != null) {
            byte[] prevPage = bufferManager.GetPage(prev);
            writePageId(prevPage, DP_NEXT_PAGE_OFFSET, next);
            bufferManager.FreePage(prev, true);
        }
        if (next != null) {
            byte[] nextPage = bufferManager.GetPage(next);
            writePageId(nextPage, DP_PREV_PAGE_OFFSET, prev);
            bufferManager.FreePage(next, true);
        }

        writePageId(page, DP_PREV_PAGE_OFFSET, null);
        writePageId(page, DP_NEXT_PAGE_OFFSET, null);
        bufferManager.FreePage(pageId, true);
        bufferManager.FreePage(headerPageId, true);
    }

    public void writeRecordToBuffer(Record record, ByteBuffer buffer, int pos) {
        buffer.position(pos);
        List<String> values = record.getValues();

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            String type = col.getType();
            String value = values.get(i);

            try {
                if (type.equals("INT")) {
                    buffer.putInt(Integer.parseInt(value));
                } else if (type.equals("FLOAT")) {
                    buffer.putFloat(Float.parseFloat(value));
                } else if (type.startsWith("CHAR(")) {
                    int size = Integer.parseInt(type.substring(5, type.length()-1));
                    for (int j = 0; j < size; j++) {
                        char c = j < value.length() ? value.charAt(j) : ' ';
                        buffer.put((byte)c);
                    }
                } else if (type.startsWith("VARCHAR(")) {
                    int maxSize = Integer.parseInt(type.substring(8, type.length()-1));
                    byte[] bytes = value.getBytes();
                    buffer.put(bytes, 0, Math.min(bytes.length, maxSize));
                    if (bytes.length < maxSize) {
                        for (int j = bytes.length; j < maxSize; j++) buffer.put((byte)0);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Erreur écriture record : " + e.getMessage());
            }
        }
    }

    public void readFromBuffer(Record record, ByteBuffer buffer, int pos) {
        buffer.position(pos);
        record.getValues().clear();

        for (ColumnInfo col : columns) {
            String type = col.getType();

            if (type.equals("INT")) {
                record.addValue(Integer.toString(buffer.getInt()));
            } else if (type.equals("FLOAT")) {
                record.addValue(Float.toString(buffer.getFloat()));
            } else if (type.startsWith("CHAR(")) {
                int size = Integer.parseInt(type.substring(5, type.length()-1));
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < size; i++) sb.append((char)buffer.get());
                record.addValue(sb.toString().trim());
            } else if (type.startsWith("VARCHAR(")) {
                int maxSize = Integer.parseInt(type.substring(8, type.length()-1));
                byte[] bytes = new byte[maxSize];
                buffer.get(bytes);
                record.addValue(new String(bytes).trim());
            }
        }
    }

}
