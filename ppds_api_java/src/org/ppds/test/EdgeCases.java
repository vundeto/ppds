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
                Map.of("file_path", "ppds_api_java/data/basic_test/table_empty.tbl"), null);
        List<Record> records = AQueryProcessorFactory.createQueryProcessor().executeQuery(emptyScan);
        assertEquals(records.size(), 0);
    }
    @Test
    public void emptyTableTest(){
        PlanNode scan = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", "ppds_api_java/data/basic_test/table_d.tbl"), null);
        PlanNode pred = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "2", "condition", "=", "literal", "1"), new PlanNode[]{scan});
        List<Record> records = AQueryProcessorFactory.createQueryProcessor().executeQuery(pred);
        assertEquals(records.size(), 0);
    }

    @Test
    public void boundaryTest1(){
        PlanNode scan = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", "ppds_api_java/data/basic_test/table_c.tbl"), null);
        PlanNode pred1 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "1", "condition", ">", "literal", "1"), new PlanNode[]{scan});
        List<Record> results1 = AQueryProcessorFactory.createQueryProcessor().executeQuery(pred1);
        assertEquals(results1.size(), 5);
    }
    @Test
    public void boundaryTest2(){
        PlanNode scan = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", "ppds_api_java/data/basic_test/table_c.tbl"), null);
        PlanNode pred2 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "1", "condition", "<", "literal", "50"), new PlanNode[]{scan});
        List<Record> results2 = AQueryProcessorFactory.createQueryProcessor().executeQuery(pred2);
        assertEquals(results2.size(), 5);
    }



}



















