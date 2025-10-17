package minisgbd;

public class ColumnInfo {

    private String name;
    private String type; // ex: "INT", "FLOAT", "CHAR(10)", "VARCHAR(20)"

    public ColumnInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return name + " : " + type;
    }
}
