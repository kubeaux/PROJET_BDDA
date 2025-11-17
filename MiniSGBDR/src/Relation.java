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

    public void addDataPage() {
        if (diskManager == null || bufferManager == null) {
            throw new IllegalStateException("DiskManager et BufferManager pas définis");
        }
        if (headerPageId == null || headerPageId.getFileIdx() < 0 || headerPageId.getPageIdx() < 0) {
            throw new IllegalStateException("HeaderPageId invalide");
        }

        byte[] headerBuffer = null;
        boolean headerDirty = false;

        try {
            PageId newPageId = diskManager.AllocPage();

            headerBuffer = bufferManager.GetPage(headerPageId);
            ByteBuffer headerView = ByteBuffer.wrap(headerBuffer);
            int freeFileIdx = headerView.getInt(8);
            int freePageIdx = headerView.getInt(12);

            PageId oldHead = null;
            if (freeFileIdx >= 0 && freePageIdx >= 0) {
                oldHead = newPageId(freeFileIdx, freePageIdx);
            }  

            byte[] newPageBuffer = bufferManager.getPage(newPageId);
            try {
                ByteBuffer newPageView = ByteBuffer.wrap(newPageBuffer);

                newPageView.putInt(0, -1);
                newPageView.putInt(4, -1);
                if (oldHead != null) {
                    newPageView.putInt(8, oldHead.getFileIdx());
                    newPageView.putInt(12, oldHead.getPageIdx);
                } else {
                    newPageView.putInt(8, -1);
                    newPageView.putInt(12, -1);
                }
                newPageView.putInt(16, 0);

                if (slotsPerPage > 0) {
                    int mapStart = DATA_PAGE_HEADER_SIZE;
                    for (int i = 0; i < slotsPerPage && mapStart + i < newPageBuffer.length; i++) {
                        newPageBuffer[mapStart + i] = 0;
                    }
                }
            } finally {
                bufferManager.FreePage(newPageId, true);
            }

            if (oldHead != null) {
                byte[] oldHeadBuffer = bufferManager.GetPage(oldHead);
                try {
                    ByteBuffer oldHeadView = ByteBuffer.wrap(oldHeadBuffer);
                    oldHeadView.putInt(0, newPageId.getFileIdx());
                    oldHeadView.putInt(4, newPageId.getPageIdx);
                } finally {
                    bufferManager.FreePage(oldHead, true);
                }
            }

            headerView.putInt(8, newPageId.getFileIdx());
            headerView.putInt(12, newPageId.getPageIdx());
            headerDirty = true;

        } catch (Exception e) {
            throw new RuntimeException("Problème lors de l'ajout d'une page de données", e);
        } finally {
            if (headerBuffer != null) {
                bufferManager.FreePage(headerPageId, headerDirty);
            }
        }
    }
}
