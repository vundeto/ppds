package org.ppds.operators;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Scans a '|'-separated table file and produces one record per line.
 *
 * <p>The scan optionally applies a projection directly while parsing
 * (see {@link #TableScan(String, int[])}): only the requested columns are
 * materialized, all other columns are skipped without allocating tokens.
 * The output record is reused across {@code next()} calls, as permitted
 * by the {@link ONCIterator} contract.</p>
 */
public class TableScan implements ONCIterator {

    private static final char SEPARATOR = '|';
    private static final int BUFFER_SIZE = 1 << 16;

    private final String path;
    /** Columns to materialize (in output order), or null for all columns. */
    private final int[] projectedColumns;

    private BufferedReader reader;
    /** Types of all columns as they appear in the file. */
    private Record.DataType[] fileSchema;
    /** Types of the columns that are actually produced. */
    private Record.DataType[] outputSchema;
    /** Maps a file column index to its output position, -1 if pruned. */
    private int[] outputPosition;
    private Record record;

    public TableScan(String path) {
        this(path, null);
    }

    public TableScan(String path, int[] projectedColumns) {
        this.path = path;
        this.projectedColumns = projectedColumns;
    }

    @Override
    public void open() {
        inferFileSchema();
        buildOutputMapping();
        try {
            reader = new BufferedReader(new FileReader(path), BUFFER_SIZE);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open table file: " + path, e);
        }
        record = new Record(outputSchema);
    }

    @Override
    public Record next() {
        String line;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read table file: " + path, e);
        }
        if (line == null) {
            return null;
        }

        int start = 0;
        int remaining = outputSchema.length;
        for (int column = 0; column < fileSchema.length && remaining > 0; column++) {
            int end = line.indexOf(SEPARATOR, start);
            if (end < 0) {
                end = line.length();
            }
            int position = outputPosition[column];
            if (position >= 0) {
                record.set(position, parseValue(fileSchema[column], line, start, end));
                remaining--;
            }
            start = end + 1;
        }
        return record;
    }

    @Override
    public void close() {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (IOException ignored) {
        } finally {
            reader = null;
        }
    }

    private static Object parseValue(Record.DataType type, String line, int start, int end) {
        return switch (type) {
            case INT32 -> Integer.parseInt(line, start, end, 10);
            case FP32 -> Float.parseFloat(line.substring(start, end));
            case String -> line.substring(start, end);
        };
    }

    /** Reads the first line of the file and guesses the type of every column. */
    private void inferFileSchema() {
        String firstLine;
        try (BufferedReader schemaReader = new BufferedReader(new FileReader(path))) {
            firstLine = schemaReader.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open table file: " + path, e);
        }

        List<Record.DataType> types = new ArrayList<>();
        if (firstLine != null) {
            int start = 0;
            while (start < firstLine.length()) {
                int end = firstLine.indexOf(SEPARATOR, start);
                if (end < 0) {
                    end = firstLine.length();
                }
                types.add(guessDataType(firstLine.substring(start, end)));
                start = end + 1;
            }
        }
        fileSchema = types.toArray(new Record.DataType[0]);
    }

    private void buildOutputMapping() {
        outputPosition = new int[fileSchema.length];
        if (projectedColumns == null) {
            outputSchema = fileSchema;
            for (int i = 0; i < fileSchema.length; i++) {
                outputPosition[i] = i;
            }
        } else {
            java.util.Arrays.fill(outputPosition, -1);
            outputSchema = new Record.DataType[projectedColumns.length];
            for (int i = 0; i < projectedColumns.length; i++) {
                outputSchema[i] = fileSchema[projectedColumns[i]];
                outputPosition[projectedColumns[i]] = i;
            }
        }
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
