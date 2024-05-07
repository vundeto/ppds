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

package org.ppds.test;

import org.junit.Test;
import org.ppds.core.PlanNode;
import org.ppds.core.PlanNode.NodeType;
import org.ppds.core.Record;
import org.ppds.core.Record.DataType;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicTest {

	@Test
	public void runThrough() {
		PlanNode scan = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "basic_test/table_a.tbl"), null);
		PlanNode pred = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "0", "condition", ">", "literal", "1"), new PlanNode[]{scan});
		PlanNode scan2 = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "basic_test/table_b.tbl"), null);
		PlanNode proj = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0"), new PlanNode[]{scan2});
		PlanNode join = new PlanNode(NodeType.EquiJoin,
			Map.of("left_column_id", "1", "right_column_id", "0"), new PlanNode[]{pred, proj});
		PlanNode groupBy = new PlanNode(NodeType.GroupBy,
			Map.of("group_by_column_id", "2", "aggregate_column_id", "0", "aggregate_type", "max"),
			new PlanNode[]{join});

		List<Record> results = AQueryProcessorFactory.createQueryProcessor().executeQuery(groupBy);
		assertEquals(results.size(), 3);
		assertEquals(results.get(0).getNumCols(), results.get(1).getNumCols());
		assertEquals(results.get(1).getNumCols(), results.get(2).getNumCols());
		assertEquals(results.get(0).getNumCols(), 2);
		assertEquals(results.get(0).getSchema(0), DataType.String);
		assertEquals(results.get(0).getSchema(1), DataType.INT32);

		String[] column_1 = {"def", "abc", "aab"};
		int[] column_2 = {4, 9, 5};

		for (int i = 0; i < column_1.length; i++) {
			boolean foundTuple = false;
			for (Record record : results) {
				String val1 = (String) record.get(0);
				int val2 = (int) record.get(1);

				if (column_1[i].equals(val1) && column_2[i] == val2) {
					foundTuple = true;
					break;
				}
			}
			assertTrue(foundTuple);
		}
	}
}
