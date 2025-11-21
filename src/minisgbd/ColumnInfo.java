package minisgbd;

public class ColumnInfo {
    private final String name;
    private final ColumnType type;
    private final int size; // Pour CHAR(T) ou VARCHAR(T)

    public ColumnInfo(String name, ColumnType type, int size) {
        this.name = name;
        this.type = type;
        this.size = size; 
    }

    public ColumnInfo(String name, ColumnType type) {
        this(name, type, 0); // INT/FLOAT
    }

    public String getName() { return name; }
    public ColumnType getType() { return type; }
    public int getSize() { return size; }
}
