package org.ppds.core;

import org.ppds.operators.TableScan;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

public class Compiler extends QueryProcessor {
    @Override
    public List<Record> executeQuery(PlanNode node) {
        return super.executeQuery(node);
    }

    @Override
    public ONCIterator compileQuery(PlanNode node) {
        if(node._type.equals(PlanNode.NodeType.TableScan)){
            try {
                return new TableScan(node._params.get("file_path"));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    public static void main(String[] args) {
        PlanNode scan = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", "data/basic_test/table_a.tbl"), null);
        Compiler c = new Compiler();
        c.executeQuery(scan);
    }
}
