package org.ppds.core;

import org.ppds.operators.Predicate;
import org.ppds.operators.Projection;
import org.ppds.operators.TableScan;
import org.ppds.operators.EquiJoin;


import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Compiler extends QueryProcessor {
    @Override
    public List<Record> executeQuery(PlanNode node) {
        return super.executeQuery(node);
    }

    @Override
    public ONCIterator compileQuery(PlanNode node) {
        try{
            if (node._type.equals(PlanNode.NodeType.TableScan)) {
                return new TableScan(node._params.get("file_path"));
            } else if (node._type.equals(PlanNode.NodeType.Predicate)) {
                return new Predicate(node._params, getIterators(node._inputs));
            } else if (node._type.equals(PlanNode.NodeType.Projection)) {
                var it = getIterators(node._inputs);
                return new Projection(node._params, it);
            }else if (node._type.equals(PlanNode.NodeType.EquiJoin)) {
                return new EquiJoin(node._params, getIterators(node._inputs));
            } else return null;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public ONCIterator[] getIterators(PlanNode[] inputs){
        return Arrays.stream(inputs).map(this::compileQuery).toArray(ONCIterator[]::new);
    }

        public static void main (String[]args){
            PlanNode scan = new PlanNode(PlanNode.NodeType.TableScan,
                    Map.of("file_path", "ppds_api_java/data/basic_test/table_a.tbl"), null);
            PlanNode pred = new PlanNode(PlanNode.NodeType.Predicate,
                    Map.of("column_id", "2", "condition", "=", "literal", "abc"), new PlanNode[]{scan});
            PlanNode proj = new PlanNode(PlanNode.NodeType.Projection,
                    Map.of("column_ids", "0,1"), new PlanNode[]{scan, pred});
            PlanNode scan2 = new PlanNode(PlanNode.NodeType.TableScan,
                    Map.of("file_path", "ppds_api_java/data/basic_test/table_b.tbl"), null);

            PlanNode EquiJoin = new PlanNode(PlanNode.NodeType.EquiJoin,
                    Map.of("left_column_id", "1", "right_column_id", "0"), new PlanNode[]{scan, scan2});

            Compiler c = new Compiler();
            c.executeQuery(EquiJoin);
        }
    }
