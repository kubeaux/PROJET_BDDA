import java.nio.ByteBuffer;
import java.util.List;

public class Relation {
    private String name;
    private List<ColumnInfo> columns;
    private PageId headerPageId;
    private int slotsPerPage;
    private DiskManager diskManager;
    private BufferManager bufferManager;

    public Relation(String name, List<ColumnInfo> columns) {
        this.name = name;
        this.columns = columns;
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.headerPageId = headerPageId;
        this.slotsPerPage = slotsPerPage;
    }

    public String getName() {
        return name;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
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
