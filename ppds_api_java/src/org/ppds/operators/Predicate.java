package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.Map;

public class Predicate implements ONCIterator {
    ONCIterator[] children;
    int column_id;
    String condition;
    String literal;

    public Predicate(Map<String, String> parameters, ONCIterator[] children) {
        column_id = Integer.parseInt(parameters.get("column_id"));
        condition = parameters.get("condition");
        literal = parameters.get("literal");
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
        if(parseData(record) != null) return new Record(record);
        else return next();
    }

    @Override
    public void close() {
        children[children.length-1].close();
    }

    public Record parseData(Record record) {
        if (record.getSchema()[column_id].equals(Record.DataType.INT32)) {
            var value_ = (int) record.get(column_id);
            var literal_ = Integer.parseInt(literal);
            if (compareNums(value_, literal_)) return new Record(record);
        } else if (record.getSchema()[column_id].equals(Record.DataType.FP32)) {
            var value_ = (float) record.get(column_id);
            var literal_ = Float.parseFloat(literal);
            if (compareNums(value_, literal_))  return new Record(record);
        } else {
            var value_ = (String) record.get(column_id);
            var literal_ = literal;
            if (compareStrings(value_, literal_))  return new Record(record);
        }
        return null;
    }


    public <T extends Object> T getLiteralValue(Record.DataType type, String literal) {
        if (type == Record.DataType.INT32) {
            return (T) Integer.valueOf(literal);
        } else if (type == Record.DataType.FP32) {
            return (T) Float.valueOf(literal);
        } else return (T) literal;
    }

    public boolean compareNums(int value, int literal) {
        return switch (condition) {
            case "=" -> value == literal;
            case ">" -> value > literal;
            case "<" -> value < literal;
            case ">=" -> value >= literal;
            case "<=" -> value <= literal;
            default -> false;
        };
    }

    public boolean compareNums(float value, float literal) {
        return switch (condition) {
            case "=" -> value == literal;
            case ">" -> value > literal;
            case "<" -> value < literal;
            case ">=" -> value >= literal;
            case "<=" -> value <= literal;
            default -> false;
        };
    }

    public boolean compareStrings(String value, String literal) {
        return switch (condition) {
            case "=" -> value.equals(literal);
            case "!=" -> !value.equals(literal);
            default -> false;
        };
    }

}





