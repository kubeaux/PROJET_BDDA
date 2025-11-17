import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

public class Relation {
    private String name;
    private List<ColumnInfo> columns;
    private PageId headerPageId;
    private int slotsPerPage;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private DBConfig dbConfig;

    private static int INVALID_FILE_IDX = -1;
    private static int INVALID_PAGE_IDX = -1;
    private static int HP_FULL_HEAD_OFFSET = 0;
    private static int HP_FREE_HEAD_OFFSET = 8;
    private static int DP_PREV_PAGE_OFFSET = 0;
    private static int DP_NEXT_PAGE_OFFSET = 8;
    private static int DP_HEADER_SIZE = 16;

    public Relation(String name, 
                    List<ColumnInfo> columns,
                    PageId headerPageId,
                    DiskManager diskManager,
                    BufferManager bufferManager,
                    DBConfig dbConfig) throws Exception {
        this.name = name;
        this.columns = columns;

        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.headerPageId = headerPageId;
        this.dbConfig = dbConfig;

        int recordSize = computeRecordSize();
        this.slotsPerPage = (dbConfig.getPageSize() - DP_HEADER_SIZE) / (recordSize + 1);
        if (this.slotsPerPage <= 0) {
            throw new Exception("Page trop petite pour stocker un record");
        }

        initHeaderPageIfNeeded();
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

    public PageId getFreeDataPageId(int sizeRecord) throws Exception {
        byte[] header = bufferManager.GetPage(headerPageId);
        PageId current = readPageId(header, HP_FREE_HEAD_OFFSET);
        bufferManager.FreePage(headerPageId, false);

        while (current != null) {
            byte[] page = bufferManager.GetPage(current);
            int bitmapStart = DP_HEADER_SIZE;

            int freeSlot = -1;
            for (int i = 0; i < slotsPerPage; i++) {
                if (page[bitmapStart + i] == 0) {
                    freeSlot = i;
                    break;
                }
            }
            bufferManager.FreePage(current, false);

            if (freeSlot != -1) {
                return current;
            }

            page = bufferManager.GetPage(current);
            PageId next = readPageId(page, DP_NEXT_PAGE_OFFSET);
            bufferManager.FreePage(current, false);
            current = next;
        }
        return null;
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

    public void addDataPage() throws Exception {
        PageId newPid = diskManager.AllocPage();

        byte[] page = bufferManager.GetPage(newPid);
        for (int i = 0; i < page.length; i++) {
            page[i] = 0;
        }

        writePageId(page, DP_PREV_PAGE_OFFSET, null);
        writePageId(page, DP_NEXT_PAGE_OFFSET, null);
        bufferManager.FreePage(newPid, true);

        addPageToList(newPid, false);
    }
}
