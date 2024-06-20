package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.util.*;

public class GroupBy implements ONCIterator {
    public enum AggrType {
        SUM, AVG, COUNT, MAX, MIN;
    }

    AggrType type;
    Iterator<Object> iterator;
    HashMap<Object, List<Object>> map;

    int aggr_column_id;
    int gb_column_id;
    ONCIterator[] children;
    Record.DataType[] schema;

    public GroupBy(Map<String, String> params, ONCIterator[] children_) {
        type = getType(params.get("AggrType"));
        if (params.containsKey("group_by_column_id")) gb_column_id = Integer.parseInt(params.get("group_by_column_id"));
        else gb_column_id = -1;
        if (params.containsKey("aggregate_column_id")) aggr_column_id= Integer.parseInt(params.get("aggregate_column_id"));
        else aggr_column_id = -1;
        children = children_;
        map = null;
    }

    @Override
    public void open() {
        children[children.length - 1].open();
    }

    @Override
    public Record next() {
        if(gb_column_id == -1){
            if(map == null)aggregateOnly();
            var aggregate = map.keySet().iterator().next();
            if (!iterator.hasNext()) return null;
            var record = new Record((Record)iterator.next());
            if(aggregate==null)return record;
            else record.set(aggr_column_id, aggregate);
            return record;
        }
        if (map == null) executeGroupBy();
        if (map.isEmpty()) return null;
        if (!iterator.hasNext()) return null;
        var key = iterator.next();
        var record = new Record(schema);
        record.set(0, key);
        record.set(1, aggregate(map.get(key), schema[1]));
        return record;
    }

    @Override
    public void close() {
        children[children.length - 1].close();
    }
    public void aggregateOnly(){
        List<Object> records = new ArrayList<>();
        List<Object> columns = new ArrayList<>();
        Record record;
        while ((record = children[children.length - 1].next()) != null) {
            records.add(record);
            columns.add(record.get(aggr_column_id));
        }
        System.out.println(records);
        System.out.println(columns);
        var r = (Record) records.get(0);
        var aggregate = aggregate(columns, r.getSchema(aggr_column_id));
        map = new HashMap<>();
        map.put(aggregate, records);
        iterator = map.get(aggregate).iterator();
    }

    public void executeGroupBy() {
        map = new HashMap<>();
        Record record;
        while ((record = children[children.length - 1].next()) != null) {
            var key = record.get(gb_column_id);
            var value = record.get(aggr_column_id);
            if (schema == null) schema = generateSchema(record);
            if (!map.containsKey(key)) {
                map.put(key, new ArrayList<>());
                map.get(key).add(value);
            } else {
                map.get(key).add(value);
            }
        }
        iterator = map.keySet().iterator();
    }

    private Record.DataType[] generateSchema(Record record) {
        var schema = new Record.DataType[2];
        schema[0] = record.getSchema(gb_column_id);
        if (type.equals(AggrType.AVG)) schema[1] = Record.DataType.FP32;
        else if (type.equals(AggrType.MAX) || type.equals(AggrType.MIN) || type.equals(AggrType.SUM))
            schema[1] = record.getSchema(aggr_column_id);
        else schema[1] = Record.DataType.INT32;
        return schema;
    }


    private Object aggregate(List<Object> values, Record.DataType dataType) {
        if (dataType == Record.DataType.FP32) {
            return switch (type) {
                case SUM -> values.stream().map(o -> (float) o).reduce((float) 0, Float::sum);
                case AVG -> values.stream().map(o -> (float) o).reduce((float) 0, Float::sum) / values.size();
                case COUNT -> values.size();
                case MAX -> values.stream().map(o -> (float) o).max(Float::compareTo).get();
                case MIN -> values.stream().map(o -> (float) o).min(Float::compareTo).get();
            };
        } else if (dataType == Record.DataType.INT32) {
            return switch (type) {
                case SUM -> values.stream().map(o -> (int) o).reduce(0, Integer::sum);
                case AVG -> values.stream().map(o -> (int) o).reduce(0, Integer::sum) / values.size();
                case COUNT -> values.size();
                case MAX -> values.stream().map(o -> (int) o).max(Integer::compareTo).get();
                case MIN -> values.stream().map(o -> (int) o).min(Integer::compareTo).get();
            };
        } else return null;
    }

    public AggrType getType(String str) {
        return switch (str) {
            case "sum" -> AggrType.SUM;
            case "avg" -> AggrType.AVG;
            case "count" -> AggrType.COUNT;
            case "max" -> AggrType.MAX;
            case "min" -> AggrType.MIN;
            default -> null;
        };
    }
}
