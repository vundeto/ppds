package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;
import java.util.*;

public class EquiJoin implements ONCIterator {
    private ONCIterator left_iterator;
    private ONCIterator right_iterator;
    private int left_column;
    private int right_column;
    private Map<Object, List<Record>> hash_table;
    private List<Record> current_right_records;
    private Record current;
    private Record.DataType[] schema;

    public EquiJoin(Map<String, String> params, ONCIterator[] inputs) {
        this.left_column = Integer.parseInt(params.get("left_column_id"));
        this.right_column = Integer.parseInt(params.get("right_column_id"));
        this.left_iterator = inputs[0];
        this.right_iterator = inputs[1];
        this.hash_table = new HashMap<>();
        this.current_right_records = null;
    }

    @Override
    public void open() {
        left_iterator.open();
        right_iterator.open();
        Record right_record;
        while ((right_record = right_iterator.next()) != null) {
            Object key = right_record.get(right_column);
            if (!hash_table.containsKey(key)) {
                hash_table.put(key, new ArrayList<>());
            }
            hash_table.get(key).add(right_record);
        }
    }

    @Override
    public Record next() {
        if (current_right_records == null) {
            while ((current = left_iterator.next()) != null) {
                Object key = current.get(left_column);
                if (hash_table.containsKey(key)) {
                    current_right_records = hash_table.get(key);
                    return createJoined( current_right_records.remove(0));
                }
            }
            return null;
        } else {
            if (!current_right_records.isEmpty()) {
                return createJoined(current_right_records.remove(0));
            } else {
                current_right_records = null;
                return next();
            }
        }
    }
    public Record createJoined(Record left){
        if(schema == null)getSchema(left);
        var record = new Record(schema);
        for (int i = 0; i < left.getSchema().length; i++) {
            record.set(i, left.get(i));
        }

        for (int i = 0; i < current.getSchema().length; i++) {
            if(i!=right_column)record.set(i + left.getSchema().length-1, current.get(i));
        }
        return record;
    }
    public void getSchema(Record left){
        var schema_ = new Record.DataType[left.getSchema().length + current.getSchema().length-1];
        for (int i = 0; i < left.getSchema().length; i++) {
            schema_[i] = left.getSchema()[i];
        }
        for (int i = 0; i < current.getSchema().length; i++) {
            if(i!=right_column)schema_[i + left.getSchema().length-1] = current.getSchema()[i];
        }
        schema = schema_;
    }
    @Override
    public void close() {
        left_iterator.close();
        right_iterator.close();
        hash_table.clear();
        current_right_records = null;
    }
}

