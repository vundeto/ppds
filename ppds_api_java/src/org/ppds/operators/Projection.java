package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.Arrays;
import java.util.Map;

/**
 * Projects the input onto the configured columns, in the given order.
 *
 * <p>The output schema is derived once from the first record and the
 * output record is reused across {@code next()} calls.</p>
 */
public class Projection implements ONCIterator {

    private final ONCIterator child;
    private final int[] columnIds;

    private Record.DataType[] outputSchema;
    private Record outputRecord;

    public Projection(Map<String, String> parameters, ONCIterator[] children) {
        columnIds = parseColumnIds(parameters);
        child = children[children.length - 1];
    }

    public static int[] parseColumnIds(Map<String, String> parameters) {
        return Arrays.stream(parameters.get("column_ids").trim().split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Record next() {
        Record record = child.next();
        if (record == null) {
            return null;
        }
        if (outputRecord == null) {
            outputSchema = buildOutputSchema(record);
            outputRecord = new Record(outputSchema);
        }
        for (int i = 0; i < columnIds.length; i++) {
            outputRecord.set(i, record.get(columnIds[i]));
        }
        return outputRecord;
    }

    @Override
    public void close() {
        child.close();
    }

    private Record.DataType[] buildOutputSchema(Record record) {
        Record.DataType[] schema = new Record.DataType[columnIds.length];
        for (int i = 0; i < schema.length; i++) {
            schema[i] = record.getSchema(columnIds[i]);
        }
        return schema;
    }
}
