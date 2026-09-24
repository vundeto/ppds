package org.ppds.test;
import org.junit.Test;
import org.ppds.core.PlanNode;
import org.ppds.core.Record;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class EdgeCases {

    @Test
    public void emptyFileTest(){
        PlanNode emptyScan = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", TestTables.TABLE_EMPTY), null);
        List<Record> records = AQueryProcessorFactory.createQueryProcessor().executeQuery(emptyScan);
        assertEquals(records.size(), 0);
    }
    @Test
    public void emptyTableTest(){
        PlanNode scan = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", TestTables.TABLE_D), null);
        PlanNode pred = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "2", "condition", "=", "literal", "1"), new PlanNode[]{scan});
        List<Record> records = AQueryProcessorFactory.createQueryProcessor().executeQuery(pred);
        assertEquals(0, records.size());
    }

    @Test
    public void boundaryTest1(){
        PlanNode scan = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", TestTables.TABLE_C), null);
        PlanNode pred1 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "1", "condition", ">", "literal", "1"), new PlanNode[]{scan});
        List<Record> results1 = AQueryProcessorFactory.createQueryProcessor().executeQuery(pred1);
        assertEquals(5, results1.size());
    }
    @Test
    public void boundaryTest2(){
        PlanNode scan = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", TestTables.TABLE_C), null);
        PlanNode pred2 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "1", "condition", "<", "literal", "50"), new PlanNode[]{scan});
        List<Record> results2 = AQueryProcessorFactory.createQueryProcessor().executeQuery(pred2);
        assertEquals(5, results2.size());
    }



}



















