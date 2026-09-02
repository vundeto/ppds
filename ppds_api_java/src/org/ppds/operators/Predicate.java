package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.List;
import java.util.Map;

/**
 * Filters records by one or more conjunctive conditions of the form
 * {@code column <op> literal}.
 *
 * <p>Each condition is compiled once (literal parsed, comparison
 * specialized to the column type) when the first record arrives, so the
 * per-record work is a single type-specialized comparison. Qualifying
 * records are passed through without copying.</p>
 */
public class Predicate implements ONCIterator {

    private enum Condition {
        EQUALS, NOT_EQUALS, GREATER, LESS, GREATER_EQUALS, LESS_EQUALS;

        static Condition fromSymbol(String symbol) {
            return switch (symbol) {
                case "=" -> EQUALS;
                case "!=" -> NOT_EQUALS;
                case ">" -> GREATER;
                case "<" -> LESS;
                case ">=" -> GREATER_EQUALS;
                case "<=" -> LESS_EQUALS;
                default -> throw new IllegalArgumentException("Unknown condition: " + symbol);
            };
        }

        boolean matches(int comparison) {
            return switch (this) {
                case EQUALS -> comparison == 0;
                case NOT_EQUALS -> comparison != 0;
                case GREATER -> comparison > 0;
                case LESS -> comparison < 0;
                case GREATER_EQUALS -> comparison >= 0;
                case LESS_EQUALS -> comparison <= 0;
            };
        }
    }

    private final ONCIterator child;
    private final int[] columnIds;
    private final Condition[] conditions;
    private final String[] literals;
    /** Compiled per-condition tests, built lazily from the first record's schema. */
    private java.util.function.Predicate<Record>[] tests;

    public Predicate(Map<String, String> parameters, ONCIterator[] children) {
        this(List.of(parameters), children[children.length - 1]);
    }

    public Predicate(List<Map<String, String>> conjunctiveConditions, ONCIterator child) {
        this.child = child;
        int n = conjunctiveConditions.size();
        columnIds = new int[n];
        conditions = new Condition[n];
        literals = new String[n];
        for (int i = 0; i < n; i++) {
            Map<String, String> parameters = conjunctiveConditions.get(i);
            columnIds[i] = Integer.parseInt(parameters.get("column_id"));
            conditions[i] = Condition.fromSymbol(parameters.get("condition"));
            literals[i] = parameters.get("literal");
        }
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Record next() {
        Record record;
        candidates:
        while ((record = child.next()) != null) {
            if (tests == null) {
                compileTests(record);
            }
            for (java.util.function.Predicate<Record> test : tests) {
                if (!test.test(record)) {
                    continue candidates;
                }
            }
            return record;
        }
        return null;
    }

    @Override
    public void close() {
        child.close();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void compileTests(Record prototype) {
        tests = new java.util.function.Predicate[conditions.length];
        for (int i = 0; i < conditions.length; i++) {
            tests[i] = compileTest(prototype.getSchema(columnIds[i]),
                    columnIds[i], conditions[i], literals[i]);
        }
    }

    private static java.util.function.Predicate<Record> compileTest(
            Record.DataType type, int columnId, Condition condition, String literal) {
        return switch (type) {
            case INT32 -> {
                int intLiteral = Integer.parseInt(literal);
                yield record -> condition.matches(Integer.compare((int) record.get(columnId), intLiteral));
            }
            case FP32 -> {
                float floatLiteral = Float.parseFloat(literal);
                yield record -> condition.matches(Float.compare((float) record.get(columnId), floatLiteral));
            }
            case String -> record -> condition.matches(((String) record.get(columnId)).compareTo(literal));
        };
    }
}
