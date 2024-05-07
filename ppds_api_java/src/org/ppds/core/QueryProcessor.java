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

import java.util.ArrayList;
import java.util.List;

public abstract class QueryProcessor {
	/**
	 * This method compiles a physical query execution plan (QEP)
	 * (directly executable) from a given logical query plan. The QEP
	 * should be composed of operators implementing the ONCIterator
	 * interface
	 * 
	 * @param node root node of the logical query plan.
	 * @return root iterator of the query execution plan.
	 */
	public abstract ONCIterator compileQuery(PlanNode node);
	
	
	/**
	 * This method compiles and executes a given logical query plan,
	 * and returns the results as a materialized list.
	 * 
	 * @param node root node of the logical query plan.
	 * @return query results
	 */
	public List<Record> executeQuery(PlanNode node) {
		//step 1: compile logical plan to physical QEP
		ONCIterator iter = compileQuery(node);

		//step 2: execute query and buffer results
		List<Record> ret = new ArrayList<>();
		Record r = null;
		iter.open();
		while( (r = iter.next()) != null ) {
			//copy record because iterators might reuse
			ret.add(new Record(r));
		}
		iter.close();
		return ret;
	}
}
