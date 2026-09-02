package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Classic in-memory hash equi-join.
 *
 * <p>The right input (by plan convention the smaller, dimension-side
 * relation) is materialized into a hash table during {@code open()}; the
 * left input then streams through and probes the table. Multiple matches
 * per probe key are emitted via a cursor into the bucket list, without
 * copying or mutating the bucket. The output record is reused across
 * {@code next()} calls.</p>
 *
 * <p>Output layout: all left columns, followed by all right columns
 * except the right join column (it is redundant with the left one).</p>
 */
public class HashJoin implements ONCIterator {

    private final ONCIterator leftInput;
    private final ONCIterator rightInput;
    private final int leftColumnId;
    private final int rightColumnId;

    private final Map<Object, List<Record>> hashTable = new HashMap<>();
    private Record currentLeft;
    private List<Record> currentMatches;
    private int matchIndex;

    private Record.DataType[] outputSchema;
    private Record outputRecord;

    public HashJoin(Map<String, String> parameters, ONCIterator[] inputs) {
        leftColumnId = Integer.parseInt(parameters.get("left_column_id"));
        rightColumnId = Integer.parseInt(parameters.get("right_column_id"));
        leftInput = inputs[0];
        rightInput = inputs[1];
    }

    @Override
    public void open() {
        leftInput.open();
        rightInput.open();
        Record rightRecord;
        while ((rightRecord = rightInput.next()) != null) {
            // copy: producers may reuse the returned record instance
            hashTable.computeIfAbsent(rightRecord.get(rightColumnId), key -> new ArrayList<>(1))
                    .add(new Record(rightRecord));
        }
    }

    @Override
    public Record next() {
        while (true) {
            if (currentMatches != null && matchIndex < currentMatches.size()) {
                return join(currentMatches.get(matchIndex++));
            }
            currentMatches = null;
            while ((currentLeft = leftInput.next()) != null) {
                List<Record> matches = hashTable.get(currentLeft.get(leftColumnId));
                if (matches != null) {
                    currentMatches = matches;
                    matchIndex = 0;
                    break;
                }
            }
            if (currentLeft == null) {
                return null;
            }
        }
    }

    @Override
    public void close() {
        leftInput.close();
        rightInput.close();
        hashTable.clear();
        currentMatches = null;
    }

    private Record join(Record right) {
        if (outputRecord == null) {
            outputSchema = buildOutputSchema(currentLeft, right);
            outputRecord = new Record(outputSchema);
        }
        int leftWidth = currentLeft.getNumCols();
        for (int i = 0; i < leftWidth; i++) {
            outputRecord.set(i, currentLeft.get(i));
        }
        int position = leftWidth;
        for (int i = 0; i < right.getNumCols(); i++) {
            if (i != rightColumnId) {
                outputRecord.set(position++, right.get(i));
            }
        }
        return outputRecord;
    }

    private Record.DataType[] buildOutputSchema(Record left, Record right) {
        Record.DataType[] schema = new Record.DataType[left.getNumCols() + right.getNumCols() - 1];
        int position = 0;
        for (int i = 0; i < left.getNumCols(); i++) {
            schema[position++] = left.getSchema(i);
        }
        for (int i = 0; i < right.getNumCols(); i++) {
            if (i != rightColumnId) {
                schema[position++] = right.getSchema(i);
            }
        }
        return schema;
    }
}
