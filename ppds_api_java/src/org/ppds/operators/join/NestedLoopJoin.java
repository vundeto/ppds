package org.ppds.operators.join;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Block nested-loop equi-join kept as a fallback operator; the compiler
 * normally emits {@link HashJoin} for equi-joins.
 *
 * <p>The right input is materialized once during {@code open()}; for each
 * left record the materialized list is scanned and every match is emitted
 * (the previous version silently dropped all but the first match per left
 * record). Output layout matches {@link HashJoin}.</p>
 */
public class NestedLoopJoin implements Join {

    private final ONCIterator leftInput;
    private final ONCIterator rightInput;
    private final int leftColumnId;
    private final int rightColumnId;

    private final List<Record> rightRecords = new ArrayList<>();
    private Record currentLeft;
    private int rightIndex;

    private Record outputRecord;

    public NestedLoopJoin(Map<String, String> parameters, ONCIterator[] inputs) {
        leftColumnId = Integer.parseInt(parameters.get("left_column_id"));
        rightColumnId = Integer.parseInt(parameters.get("right_column_id"));
        leftInput = inputs[0];
        rightInput = inputs[1];
    }

    @Override
    public int getRightColumnId() {
        return rightColumnId;
    }

    @Override
    public Record getCurrentLeft() {
        return currentLeft;
    }

    @Override
    public Record getOutputRecord() {
        return outputRecord;
    }

    @Override
    public void setOutputRecord(Record outputRecord) {
        this.outputRecord = outputRecord;
    }

    @Override
    public void open() {
        leftInput.open();
        rightInput.open();
        Record rightRecord;
        while ((rightRecord = rightInput.next()) != null) {
            // copy: producers may reuse the returned record instance
            rightRecords.add(new Record(rightRecord));
        }
    }

    @Override
    public Record next() {
        while (true) {
            if (currentLeft == null) {
                currentLeft = leftInput.next();
                if (currentLeft == null) {
                    return null;
                }
                rightIndex = 0;
            }
            Object leftKey = currentLeft.get(leftColumnId);
            while (rightIndex < rightRecords.size()) {
                Record right = rightRecords.get(rightIndex++);
                if (leftKey.equals(right.get(rightColumnId))) {
                    return join(right);
                }
            }
            currentLeft = null;
        }
    }

    @Override
    public void close() {
        leftInput.close();
        rightInput.close();
        rightRecords.clear();
    }
}
