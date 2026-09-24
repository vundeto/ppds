package org.ppds.operators.join;

import org.ppds.core.ONCIterator;
import org.ppds.core.Record;

/**
 * Shared equi-join output: all left columns, then all right columns
 * except the right join key.
 */
public interface Join extends ONCIterator {
    
	int getRightColumnId();

	Record getCurrentLeft();

	Record getOutputRecord();

	void setOutputRecord(Record outputRecord);

	default Record.DataType[] buildOutputSchema(Record left, Record right) {
		Record.DataType[] schema = new Record.DataType[left.getNumCols() + right.getNumCols() - 1];
		int position = 0;
		for (int i = 0; i < left.getNumCols(); i++) {
			schema[position++] = left.getSchema(i);
		}
		for (int i = 0; i < right.getNumCols(); i++) {
			if (i != getRightColumnId()) {
				schema[position++] = right.getSchema(i);
			}
		}
		return schema;
	}

	default Record join(Record right) {
		Record outputRecord = getOutputRecord();
		if (outputRecord == null) {
			outputRecord = new Record(buildOutputSchema(getCurrentLeft(), right));
			setOutputRecord(outputRecord);
		}
		Record currentLeft = getCurrentLeft();
		int leftWidth = currentLeft.getNumCols();
		for (int i = 0; i < leftWidth; i++) {
			outputRecord.set(i, currentLeft.get(i));
		}
		int position = leftWidth;
		for (int i = 0; i < right.getNumCols(); i++) {
			if (i != getRightColumnId()) {
				outputRecord.set(position++, right.get(i));
			}
		}
		return outputRecord;
	}
}
