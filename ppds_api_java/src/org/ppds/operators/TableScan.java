package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class TableScan implements ONCIterator {

    final char separator = '|';

    String path;
    BufferedReader reader;
    List<Record.DataType> dataTypes;

    public TableScan(String path) throws FileNotFoundException {
        this.path = path;
        FileInputStream fis = new FileInputStream(path);
        reader = new BufferedReader(new InputStreamReader(fis));
        dataTypes = new ArrayList<>();
    }


    @Override
    public void open() {
        try {
            FileInputStream fis = new FileInputStream(path);
            var br = new BufferedReader(new InputStreamReader(fis));
            var line = br.readLine().toCharArray();
            var token = "";
            for (char c : line) {
                if (c == separator || c == line[line.length - 1]) {
                    var type = guessDataType(token);
                    dataTypes.add(type);
                    token = "";
                }else token += c;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Record next() {
        var schema = new Record.DataType[dataTypes.size()];
        for (int i = 0; i < schema.length; i++) {
            schema[i] = dataTypes.get(i);
        }
        var columnIndex = 0;
        var record = new Record(schema);
        try{
            var line = reader.readLine().toCharArray();
            var token = "";
            for (char c : line) {
                if (c == separator ||c == line[line.length - 1]) {
                    parseData(record, columnIndex, token);
                    token = "";
                    columnIndex++;
                }else token += c;
            }
        }catch(Exception e){
            return null;
        }
        return record;
    }

    @Override
    public void close() {
        try {
            reader.close();
        }catch(Exception e){
        }
    }

    public void parseData(Record record, int index, String token) {
        if (record.getSchema(index).equals(Record.DataType.INT32)) {
            record.set(index, Integer.parseInt(token));
        } else if (record.getSchema(index).equals(Record.DataType.FP32)) {
            record.set(index, Float.parseFloat(token));
        } else record.set(index, token);

    }

    public static Record.DataType guessDataType(String token) {
        try {
            Integer.parseInt(token);
            return Record.DataType.INT32;
        } catch (NumberFormatException e) {
            try {
                Float.parseFloat(token);
                return Record.DataType.FP32;
            } catch (NumberFormatException e1) {
                return Record.DataType.String;
            }
        }
    }
}
