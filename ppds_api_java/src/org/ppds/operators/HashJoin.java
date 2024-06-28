package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashJoin implements ONCIterator {
    ONCIterator left_iterator;
    ONCIterator right_iterator;
    int left_column;
    int right_column;
    Map<Object, List<Record>> hash_table;
    List<Record> current_right_records;
    Record current;
    Record.DataType[] schema;

    public HashJoin(Map<String, String> params, ONCIterator[] inputs) {
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
                    current_right_records = new ArrayList<>(hash_table.get(key));
                    if (current_right_records.isEmpty()) return null;
                    else return createJoined(current_right_records.remove(0));
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

    public Record createJoined(Record right) {
        if (schema == null) getSchema(right);
        var record = new Record(schema);
        for (int i = 0; i < current.getSchema().length; i++) {
            record.set(i, current.get(i));
        }
        var index = current.getSchema().length - 1;
        for (int i = 0; i < right.getSchema().length; i++) {
            if (i != right_column) {
                index++;
                record.set(index, right.get(i));
            }
        }
        return record;
    }

    @Override
    public void close() {
        left_iterator.close();
        right_iterator.close();
        hash_table.clear();
        current_right_records = null;
    }

    public void getSchema(Record right) {
        var schema_ = new Record.DataType[right.getSchema().length + current.getSchema().length - 1];
        for (int i = 0; i < current.getSchema().length; i++) {
            schema_[i] = current.getSchema()[i];
        }
        var index = current.getSchema().length - 1;
        for (int i = 0; i < right.getSchema().length; i++) {
            if (i != right_column) {
                index++;
                schema_[index] = right.getSchema()[i];
            }

            schema = schema_;
        }
    }
}
