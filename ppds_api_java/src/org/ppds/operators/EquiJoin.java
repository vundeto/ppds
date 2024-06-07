package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;
import java.util.*;

public class EquiJoin implements ONCIterator {
    private ONCIterator left_iterator;
    private ONCIterator right_iterator;
    private String left_column;
    private String right_column;
    private Map<Object, List<Record>> hash_table;
    private List<Record> current_left_records;

    public EquiJoin(String leftColumn, String rightColumn, ONCIterator leftIterator, ONCIterator rightIterator) {
        this.left_column = leftColumn;
        this.right_column = rightColumn;
        this.left_iterator = leftIterator;
        this.right_iterator = rightIterator;
        this.hash_table = new HashMap<>();
        this.current_left_records = null;
    }

    @Override
    public void open() {
        left_iterator.open();
        right_iterator.open();
        Record right_record;
        while ((right_record = right_iterator.next()) != null) {
            Object key = right_record.get(Integer.parseInt(right_column));
            if (!hash_table.containsKey(key)) {
                hash_table.put(key, new ArrayList<>());
            }
            hash_table.get(key).add(right_record);
        }
    }

    @Override
    public Record next() {
        if (current_left_records == null) {
            Record leftRecord;
            while ((leftRecord = left_iterator.next()) != null) {
                Object key = leftRecord.get(Integer.parseInt(left_column));
                if (hash_table.containsKey(key)) {
                    current_left_records = hash_table.get(key);
                    return leftRecord;
                }
            }
            return null;
        } else {
            if (!current_left_records.isEmpty()) {
                return current_left_records.remove(0);
            } else {
                current_left_records = null;
                return next();
            }
        }
    }

    @Override
    public void close() {
        left_iterator.close();
        right_iterator.close();
        hash_table.clear();
        current_left_records = null;
    }
}

