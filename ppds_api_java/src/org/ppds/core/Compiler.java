package org.ppds.core;

import org.ppds.operators.*;


import java.util.*;

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
            } else if (node._type.equals(PlanNode.NodeType.GroupBy)) {
                return new GroupBy(node._params, getIterators(node._inputs));
            }else return null;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public ONCIterator[] getIterators(PlanNode[] inputs){
        return Arrays.stream(inputs).map(this::compileQuery).toArray(ONCIterator[]::new);
    }
    public static PlanNode q1_1LQP(String param1, String param2) {
        PlanNode getLO = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", "ppds_api_java/data/benchmark/lineorder.tbl"), null);
        PlanNode projLO = new PlanNode(PlanNode.NodeType.Projection,
                Map.of("column_ids", "5,8,11,13"), new PlanNode[]{getLO});
        PlanNode predLO1 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "2", "condition", ">=", "literal", param1), new PlanNode[]{projLO});
        PlanNode predLO2 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "2", "condition", "<=", "literal", param2), new PlanNode[]{predLO1});
        PlanNode predLO3 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "1", "condition", ">", "literal", "25"), new PlanNode[]{predLO2});


        PlanNode getD = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", "ppds_api_java/data/benchmark/date.tbl"), null);
        PlanNode projD = new PlanNode(PlanNode.NodeType.Projection,
                Map.of("column_ids", "0,4"), new PlanNode[]{getD});
        PlanNode predD1 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "1", "condition", "=", "literal", "1993"), new PlanNode[]{projD});
        PlanNode join = new PlanNode(PlanNode.NodeType.EquiJoin,
                Map.of("left_column_id", "0", "right_column_id", "0"), new PlanNode[]{predLO3, predD1});
        PlanNode groupBy = new PlanNode(PlanNode.NodeType.GroupBy,
                Map.of("aggregate_column_id", "3", "AggrType", "avg"), new PlanNode[]{join});

        return groupBy;
    }

        public static void main (String[]args){


            Compiler c = new Compiler();
            PlanNode node = q1_1LQP("1", "3");
            c.executeQuery(node);
        }
    }
