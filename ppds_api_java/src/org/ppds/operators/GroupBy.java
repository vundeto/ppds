package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Hash-based aggregation with an optional grouping column.
 *
 * <p>Aggregates are maintained as running accumulators (count, sum, min,
 * max) in a single pass over the input, so memory usage is proportional
 * to the number of groups instead of the number of input records.</p>
 *
 * <p>With a grouping column the output is one record per group with the
 * layout {@code (group key, aggregate)}. Without a grouping column the
 * whole input forms one group and exactly one single-column record
 * holding the aggregate is produced (none if the input is empty).</p>
 */
public class GroupBy implements ONCIterator {

    public enum AggregateType {
        SUM, AVG, COUNT, MAX, MIN;

        static AggregateType fromString(String name) {
            return switch (name) {
                case "sum" -> SUM;
                case "avg" -> AVG;
                case "count" -> COUNT;
                case "max" -> MAX;
                case "min" -> MIN;
                default -> throw new IllegalArgumentException("Unknown aggregate type: " + name);
            };
        }
    }

    /** Running per-group state; covers all supported aggregate types. */
    private static final class Accumulator {
        long count;
        double sum;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        void add(double value) {
            count++;
            sum += value;
            if (value < min) min = value;
            if (value > max) max = value;
        }
    }

    private static final int NO_COLUMN = -1;

    private final ONCIterator child;
    private final AggregateType aggregateType;
    private final int groupByColumnId;
    private final int aggregateColumnId;

    /** Insertion-ordered for deterministic output order. */
    private LinkedHashMap<Object, Accumulator> groups;
    private Iterator<Map.Entry<Object, Accumulator>> groupIterator;
    private Record.DataType aggregateInputType;
    private Record outputRecord;

    public GroupBy(Map<String, String> parameters, ONCIterator[] children) {
        // some plans use the key "AggrType" instead of "aggregate_type"
        String aggregateName = parameters.containsKey("aggregate_type")
                ? parameters.get("aggregate_type") : parameters.get("AggrType");
        aggregateType = AggregateType.fromString(aggregateName);
        groupByColumnId = parameters.containsKey("group_by_column_id")
                ? Integer.parseInt(parameters.get("group_by_column_id")) : NO_COLUMN;
        aggregateColumnId = parameters.containsKey("aggregate_column_id")
                ? Integer.parseInt(parameters.get("aggregate_column_id")) : NO_COLUMN;
        child = children[children.length - 1];
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Record next() {
        if (groups == null) {
            consumeInput();
        }
        if (!groupIterator.hasNext()) {
            return null;
        }
        Map.Entry<Object, Accumulator> group = groupIterator.next();
        if (groupByColumnId == NO_COLUMN) {
            outputRecord.set(0, aggregateResult(group.getValue()));
        } else {
            outputRecord.set(0, group.getKey());
            outputRecord.set(1, aggregateResult(group.getValue()));
        }
        return outputRecord;
    }

    @Override
    public void close() {
        child.close();
    }

    /** Builds all group accumulators in a single pass over the input. */
    private void consumeInput() {
        groups = new LinkedHashMap<>();
        Record record;
        while ((record = child.next()) != null) {
            if (outputRecord == null) {
                aggregateInputType = record.getSchema(aggregateColumnId);
                outputRecord = new Record(buildOutputSchema(record));
            }
            Object key = groupByColumnId == NO_COLUMN ? Boolean.TRUE : record.get(groupByColumnId);
            Accumulator accumulator = groups.computeIfAbsent(key, k -> new Accumulator());
            Object value = record.get(aggregateColumnId);
            if (value instanceof Number number) {
                accumulator.add(number.doubleValue());
            } else {
                accumulator.count++;
            }
        }
        groupIterator = groups.entrySet().iterator();
    }

    private Object aggregateResult(Accumulator accumulator) {
        return switch (aggregateType) {
            case COUNT -> (int) accumulator.count;
            case AVG -> (float) (accumulator.sum / accumulator.count);
            case SUM -> toInputType(accumulator.sum);
            case MIN -> toInputType(accumulator.min);
            case MAX -> toInputType(accumulator.max);
        };
    }

    private Object toInputType(double value) {
        return switch (aggregateInputType) {
            case INT32 -> (int) value;
            case FP32 -> (float) value;
            case String -> null; // non-numeric columns cannot be summed/compared
        };
    }

    private Record.DataType[] buildOutputSchema(Record record) {
        Record.DataType aggregateOutputType = switch (aggregateType) {
            case COUNT -> Record.DataType.INT32;
            case AVG -> Record.DataType.FP32;
            case SUM, MIN, MAX -> aggregateInputType;
        };
        if (groupByColumnId == NO_COLUMN) {
            return new Record.DataType[]{aggregateOutputType};
        }
        return new Record.DataType[]{record.getSchema(groupByColumnId), aggregateOutputType};
    }
}
