package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.Arrays;
import java.util.Map;

public class Projection implements ONCIterator{
    ONCIterator[] children;
    int[] column_ids;

    public Projection(Map<String, String> parameters, ONCIterator[] children) {
        column_ids = Arrays.stream(parameters.get("column_ids").trim().split(","))
            .mapToInt(Integer::parseInt)
            .toArray();
        this.children = children;
    }

    @Override
    public void open() {
        children[children.length-1].open();
    }

    @Override
    public Record next() {
        var record = children[children.length-1].next();
        if(record == null) return null;
        var proj = new Record(getSchema(record));
        for(int i = 0; i< column_ids.length; i++) {
            proj.set(i, record.get(column_ids[i]));
        }
        return proj;
    }
    public Record.DataType[] getSchema(Record record) {
        var schema = new Record.DataType[column_ids.length];
        for (int i = 0; i < schema.length; i++) {
            schema[i] = record.getSchema(column_ids[i]);
        }
        return schema;
    }
    @Override
    public void close() {
        children[0].close();
    }
}
