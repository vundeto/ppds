package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class TableScan implements ONCIterator {

    final char separator = '|';

    String path;
    BufferedReader reader;
    List<Record.DataType> dataTypes;
    Record.DataType[] schema;
    Function<String, Object>[] parsingFuncs;

    public TableScan(String path) throws FileNotFoundException {
        this.path = path;
        FileInputStream fis = new FileInputStream(path);
        reader = new BufferedReader(new InputStreamReader(fis));
        dataTypes = new ArrayList<>();
    }


    @Override
    public void open() {
        try (var br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))){
            var line = br.readLine().toCharArray();
            var token = new StringBuilder();
            for (char c : line) {
                if (c == separator || c == line[line.length - 1]) {
                    var type = guessDataType(token.toString());
                    dataTypes.add(type);
                    token.setLength(0);
                }else token.append(c);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        schema = createSchema();
        parsingFuncs = getParsingFuncs(schema);
    }

    @Override
    public Record next() {

        var columnIndex = 0;
        var record = new Record(schema);
        try{
            var line = reader.readLine().toCharArray();
            var token = new StringBuilder();
            for (char c : line) {
                if (c == separator ||c == line[line.length - 1]) {
                    parseData(record, columnIndex, token.toString());
                    token.setLength(0);
                    columnIndex++;
                }else token.append(c);
            }
        }catch(Exception e){
            if(e instanceof NullPointerException) return null;//end of file
            else e.printStackTrace();
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
    public void parseData_(Record record, int index, String token){

    }
    public void parseData(Record record, int index, String token) {
        record.set(index, parsingFuncs[index].apply(token));
    }
    public Function<String, Object>[] getParsingFuncs(Record.DataType[] schema){
        Function<String, Object>[] funcs = new Function[schema.length];
        for (int i = 0; i < schema.length; i++) {
            funcs[i] = getFunc(schema[i]);
        }
        return funcs;
    }
    public Function<String, Object> getFunc(Record.DataType type){
        return switch (type) {
            case INT32 -> Integer::parseInt;
            case FP32 -> Float::parseFloat;
            case String -> str->str;
        };
    }
    public Record.DataType[] createSchema(){
        schema = new Record.DataType[dataTypes.size()];
        for (int i = 0; i < dataTypes.size(); i++) {
            schema[i] = dataTypes.get(i);
        }
        return schema;
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
