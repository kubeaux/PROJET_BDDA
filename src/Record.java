package minisgbd;

import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<String> values;

    public Record() {
        values = new ArrayList<>();
    }

    public Record(List<String> values) {
        this.values = values;
    }

    public List<String> getValues() {
        return values;
    }

    public void addValue(String value) {
        values.add(value);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
