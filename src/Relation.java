package minisgbd;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Relation {
    private String name;
    private List<ColumnInfo> columns;

    public Relation(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
    }

    public void addColumn(String columnName, String columnType) {
        columns.add(new ColumnInfo(columnName, columnType));
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public String getName() {
        return name;
    }

    /**
     * Écrit un record dans un buffer à partir de la position pos.
     */
    public void writeRecordToBuffer(Record record, ByteBuffer buff, int pos) {
        buff.position(pos);

        for (int i = 0; i < columns.size(); i++) {
            String type = columns.get(i).getType().toUpperCase();
            String value = record.getValues().get(i);

            switch (getBaseType(type)) {
                case "INT":
                    buff.putInt(Integer.parseInt(value));
                    break;

                case "FLOAT":
                    buff.putFloat(Float.parseFloat(value));
                    break;

                case "CHAR":
                    int sizeChar = getSize(type);
                    byte[] chars = value.getBytes(StandardCharsets.UTF_8);
                    byte[] padded = new byte[sizeChar];
                    System.arraycopy(chars, 0, padded, 0, Math.min(chars.length, sizeChar));
                    buff.put(padded);
                    break;

                case "VARCHAR":
                    int maxSize = getSize(type);
                    byte[] strBytes = value.getBytes(StandardCharsets.UTF_8);
                    int actualLength = Math.min(strBytes.length, maxSize);
                    buff.putInt(actualLength); // stocker la longueur
                    buff.put(strBytes, 0, actualLength);
                    break;
            }
        }
    }

    /**
     * Lit un record depuis un buffer à partir de la position pos.
     */
    public void readFromBuffer(Record record, ByteBuffer buff, int pos) {
        buff.position(pos);
        record.getValues().clear();

        for (ColumnInfo col : columns) {
            String type = col.getType().toUpperCase();

            switch (getBaseType(type)) {
                case "INT":
                    record.addValue(String.valueOf(buff.getInt()));
                    break;

                case "FLOAT":
                    record.addValue(String.valueOf(buff.getFloat()));
                    break;

                case "CHAR":
                    int sizeChar = getSize(type);
                    byte[] charBytes = new byte[sizeChar];
                    buff.get(charBytes);
                    record.addValue(new String(charBytes, StandardCharsets.UTF_8).trim());
                    break;

                case "VARCHAR":
                    int length = buff.getInt();
                    byte[] varcharBytes = new byte[length];
                    buff.get(varcharBytes);
                    record.addValue(new String(varcharBytes, StandardCharsets.UTF_8));
                    break;
            }
        }
    }

    // Helper methods
    private String getBaseType(String type) {
        if (type.startsWith("CHAR")) return "CHAR";
        if (type.startsWith("VARCHAR")) return "VARCHAR";
        return type;
    }

    private int getSize(String type) {
        int start = type.indexOf('(');
        int end = type.indexOf(')');
        if (start != -1 && end != -1) {
            return Integer.parseInt(type.substring(start + 1, end));
        }
        return 0;
    }
}
