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

public class Record {
	public enum DataType {
		INT32,
		FP32,
		String
	}
	
	protected DataType[] _schema;
	protected Object[] _data;

	public Record(DataType[] schema) {
		_schema = schema;
		_data = new Object[schema.length];
	}
	
	public Record(Record that) {
		// deep copy of passed record arrays
		_schema = that._schema.clone();
		_data = that._data.clone();
	}
	
	public Object get(int index) {
		return _data[index];
	}

	public void set(int index, Object o) {
		_data[index] = o;
	}

	public int getNumCols() {
		return _schema.length;
	}

	public DataType[] getSchema() {
		return _schema;
	}
	
	public DataType getSchema(int index) {
		return _schema[index];
	}
}
