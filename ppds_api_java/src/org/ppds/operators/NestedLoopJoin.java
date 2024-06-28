package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NestedLoopJoin implements ONCIterator{

    ONCIterator left_iterator;
    ONCIterator right_iterator;
    int left_column;
    int right_column;
    List<Record> right_records;
    Record current;
    Record.DataType[] schema;

    public NestedLoopJoin(Map<String, String> params, ONCIterator[] inputs) {
        this.left_column = Integer.parseInt(params.get("left_column_id"));
        this.right_column = Integer.parseInt(params.get("right_column_id"));
        this.left_iterator = inputs[0];
        this.right_iterator = inputs[1];
        this.right_records = new ArrayList<>();

    }
    @Override
    public void open() {
        left_iterator.open();
        right_iterator.open();
        Record right_record;
        while ((right_record = right_iterator.next()) != null) {
            right_records.add(right_record);
        }
    }

    @Override
    public Record next() {
        while ((current = left_iterator.next()) != null) {
            for (Record right_record : right_records) {
                if (current.get(left_column).equals(right_record.get(right_column))) {
                    return createJoined(right_record);
                }
            }
        }
        return null;
    }
    public Record createJoined(Record right) {
        if (schema == null) createSchema(right);
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



    public void createSchema(Record right) {
        schema = new Record.DataType[right.getSchema().length + current.getSchema().length - 1];
        setData(right, current, schema, right_column);
    }
    private void setData(Record right, Record current, Object[] arr, int rightColumn) {
        for (int i = 0; i < current.getSchema().length; i++) {
            arr[i] = current.getSchema()[i];
        }
        var index = current.getSchema().length - 1;
        for (int i = 0; i < right.getSchema().length; i++) {
            if (i != rightColumn) {
                index++;
                arr[index] = right.getSchema()[i];
            }
        }
    }


    @Override
    public void close() {
        left_iterator.close();
        right_iterator.close();
    }

}
