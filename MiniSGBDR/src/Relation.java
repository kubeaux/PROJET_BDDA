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

    public RecordId writeRecordToDataPage(Record record, PageId pageId) throws Exception {
        byte[] pageData = bufferManager.GetPage(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(pageData);

        int bytemapOffset = 20;
        int slotFound = -1;

        for (int i = 0; i < slotCount; i++) {
            if (buffer.get(bytemapOffset + i) == 0) {
                slotFound = i;
                buffer.put(bytemapOffset + i, (byte) 1);
                break;
            }
        }

        if (slotFound == -1) {
            bufferManager.FreePage(pageId, false);
            throw new Exception("Erreur: Pas de slot libre sur la page fournie");
        }

        int dataStartOffset = bytemapOffset + slotCount;
        int recordOffset = dataStartOffset + (slotFound * recordSize);

        writeRecordToBuffer(record, buffer, recordOffset);

        bufferManager.FreePage(pageId, true);
        return new RecordId(pageId, slotFound);
    }

    public List<Record> getRecordsInDataPage(PageId pageId) throws Exception {
        List<Record> records = new ArrayList<>();
        byte[] pageData = bufferManager.GetPage(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(pageData);

        int bytemapOffset = 20;
        int dataStartOffset = bytemapOffset + slotCount;

        for (int i = 0; i < slotCount; i++) {
            if (buffer.get(bytemapOffset + i) == 1) {
                int recordOffset = dataStartOffset + (i * recordSize);
                Record r = new Record();
                readFromBuffer(r, buffer, recordOffset);
                records.add(r);
            }
        }

        bufferManager.FreePage(pageId, false);
        return records;
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

}
