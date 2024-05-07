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
import org.ppds.core.QueryProcessor;
import org.ppds.core.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.lang.Pair;

public class SpeedTest {
	private static final int ITERATIONS = 5;

	
	public static PlanNode q1_1LQP() {
		return q1_1LQP("1", "3");
	}

	public static PlanNode q1_1LQP(String param1, String param2) {
		PlanNode getLO = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/lineorder.tbl"), null);
		PlanNode projLO = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "5,8,11,13"), new PlanNode[]{getLO});
		PlanNode predLO1 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "2", "condition", ">=", "literal", param1), new PlanNode[]{projLO});
		PlanNode predLO2 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "2", "condition", "<=", "literal", param2), new PlanNode[]{predLO1});
		PlanNode predLO3 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", ">", "literal", "25"), new PlanNode[]{predLO2});


		PlanNode getD = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/date.tbl"), null);
		PlanNode projD = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0,4"), new PlanNode[]{getD});
		PlanNode predD1 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", "=", "literal", "1993"), new PlanNode[]{projD});
		PlanNode join = new PlanNode(NodeType.EquiJoin,
			Map.of("left_column_id", "0", "right_column_id", "0"), new PlanNode[]{predLO3, predD1});
		PlanNode groupBy = new PlanNode(NodeType.GroupBy,
			Map.of("aggregate_column_id", "3", "aggregate_type", "avg"), new PlanNode[]{join});

		return groupBy;
	}

	public static PlanNode q1_2LQP() {
		return q1_2LQP("26", "35");
	}

	public static PlanNode q1_2LQP(String param1, String param2) {
		PlanNode getLO = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/lineorder.tbl"), null);
		PlanNode projLO = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "5,8,11,13"), new PlanNode[]{getLO});
		PlanNode predLO1 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "2", "condition", ">=", "literal", "4"), new PlanNode[]{projLO});
		PlanNode predLO2 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "2", "condition", "<=", "literal", "6"), new PlanNode[]{predLO1});
		PlanNode predLO3 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", ">", "literal", param1), new PlanNode[]{predLO2});
		PlanNode predLO4 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", ">", "literal", param2), new PlanNode[]{predLO3});

		PlanNode getD = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/date.tbl"), null);
		PlanNode projD = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0,5"), new PlanNode[]{getD});
		PlanNode predD1 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", "=", "literal", "199401"), new PlanNode[]{projD});

		PlanNode join = new PlanNode(NodeType.EquiJoin, Map.of("left_column_id", "0", "right_column_id", "0"), new PlanNode[]{predLO4, predD1});
		PlanNode groupBy = new PlanNode(NodeType.GroupBy, Map.of("aggregate_column_id", "3", "aggregate_type", "avg"), new PlanNode[]{join});

		return groupBy;
	}

	public static PlanNode q2_1LQP() {
		return q2_1LQP("MFGR#12");
	}

	public static PlanNode q2_1LQP(String param1) {
		PlanNode getLO = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/lineorder.tbl"), null);
		PlanNode projLO = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "3,4,5,12"), new PlanNode[]{getLO});

		PlanNode getD = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/date.tbl"), null);
		PlanNode projD = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0,4"), new PlanNode[]{getD});

		PlanNode getP = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/part.tbl"), null);
		PlanNode projP = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0,3"), new PlanNode[]{getP});
		PlanNode predP1 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", "=", "literal", param1), new PlanNode[]{projP});

		PlanNode getS = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/supplier.tbl"), null);
		PlanNode projS = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0,5"), new PlanNode[]{getS});
		PlanNode predS1 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", "=", "literal", "AMERICA"), new PlanNode[]{projS});

		PlanNode join1 = new PlanNode(NodeType.EquiJoin,
			Map.of("left_column_id", "2", "right_column_id", "0"), new PlanNode[]{projLO, projD});
		PlanNode join2 = new PlanNode(NodeType.EquiJoin,
			Map.of("left_column_id", "0", "right_column_id", "0"), new PlanNode[]{join1, predP1});
		PlanNode join3 = new PlanNode(NodeType.EquiJoin,
			Map.of("left_column_id", "1", "right_column_id", "0"), new PlanNode[]{join2, predS1});
		PlanNode groupBy = new PlanNode(NodeType.GroupBy,
			Map.of("group_by_column_id", "5", "aggregate_column_id", "3", "aggregate_type", "max"), new PlanNode[]{join3});

		return groupBy;
	}

	static PlanNode q2_2LQP() {
		return q2_2LQP("MFGR#2239");
	}

	static PlanNode q2_2LQP(String param1) {
		PlanNode getLO = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/lineorder.tbl"), null);
		PlanNode projLO = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "3,4,5,12"), new PlanNode[]{getLO});

		PlanNode getD = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/date.tbl"), null);
		PlanNode projD = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0,4"), new PlanNode[]{getD});

		PlanNode getP = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/part.tbl"), null);
		PlanNode projP = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0,4"), new PlanNode[]{getP});
		PlanNode predP1 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", "=", "literal", param1), new PlanNode[]{projP});

		PlanNode getS = new PlanNode(NodeType.TableScan,
			Map.of("file_path", "benchmark/supplier.tbl"), null);
		PlanNode projS = new PlanNode(NodeType.Projection,
			Map.of("column_ids", "0,5"), new PlanNode[]{getS});
		PlanNode predS1 = new PlanNode(NodeType.Predicate,
			Map.of("column_id", "1", "condition", "=", "literal", "EUROPE"), new PlanNode[]{projS});

		PlanNode join1 = new PlanNode(NodeType.EquiJoin,
			Map.of("left_column_id", "2", "right_column_id", "0"), new PlanNode[]{projLO, projD});
		PlanNode join2 = new PlanNode(NodeType.EquiJoin,
			Map.of("left_column_id", "0", "right_column_id", "0"), new PlanNode[]{join1, predP1});
		PlanNode join3 = new PlanNode(NodeType.EquiJoin,
			Map.of("left_column_id", "1", "right_column_id", "0"), new PlanNode[]{join2, predS1});
		PlanNode groupBy = new PlanNode(NodeType.GroupBy,
			Map.of("group_by_column_id", "5", "aggregate_column_id", "3", "aggregate_type", "min"), new PlanNode[]{join3});

		return groupBy;
	}

	@Test
	public void RunBenchmark() {
		System.out.println("### Benchmark ###");

		ArrayList<Pair<String, PlanNode>> lqps = new ArrayList<>(Arrays.asList(
				Pair.of("Q1.1", q1_1LQP()),
				Pair.of("Q1.2", q1_2LQP()),
				Pair.of("Q2.1", q2_1LQP()),
				Pair.of("Q2.2", q2_2LQP())
		));

		QueryProcessor proc = AQueryProcessorFactory.createQueryProcessor();
		
		double total_time = 0;
		for (Pair<String, PlanNode> lqp : lqps) {
			for (int i = 0; i < ITERATIONS; i++) {
				long begin = System.currentTimeMillis();
				List<Record> results = proc.executeQuery(lqp.getRight());
				long end = System.currentTimeMillis();
				double time = ((double)end - begin) / 1000;
				total_time += time;
				System.out.println(lqp.getLeft() + " | " + i + " | " + time + " s");
			}
		}
		System.out.println("Total time: " + total_time);
	}
}
