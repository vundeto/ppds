/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.ppds.core;

import java.util.Arrays;

public class Record {
	public enum DataType {
		INT32,
		FP32,
		String
	}
	
	protected DataType[] schema;
	protected Object[] data;

	public Record(DataType[] schema) {
		this.schema = schema;
		this.data = new Object[schema.length];
	}
	
	public Record(Record that) {
		// deep copy of passed record arrays
		schema = that.schema.clone();
		data = that.data.clone();
	}
	
	public Object get(int index) {
		return data[index];
	}

	public void set(int index, Object o) {
		data[index] = o;
	}

	public int getNumCols() {
		return schema.length;
	}

	public DataType[] getSchema() {
		return schema;
	}
	
	public DataType getSchema(int index) {
		return schema[index];
	}

	@Override
	public String toString() {
		return "Record{" +
				"schema=" + Arrays.toString(schema) +
				", data=" + Arrays.toString(data) +
				'}';
	}
}
