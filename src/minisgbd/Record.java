package minisgbd;

import java.util.ArrayList;
import java.util.List;

public class Record {

    public List<String> values = new ArrayList<>();

    public Record() {
        this.values = new ArrayList<>();
    }


    public Record(List<String> initialValues) {
        values.addAll(initialValues);
    }


    public void addValue(String val) {
        values.add(val);
    }

    public List<String> getValues() {
        return values;
    }

    public void clear() {
        values.clear();
    }
    @Override
    public String toString() {
        return values.toString();
    }
}
