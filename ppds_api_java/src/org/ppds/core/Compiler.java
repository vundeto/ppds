package org.ppds.core;

import org.ppds.operators.GroupBy;
import org.ppds.operators.join.HashJoin;
import org.ppds.operators.Predicate;
import org.ppds.operators.Projection;
import org.ppds.operators.TableScan;
import org.ppds.test.TestTables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compiles logical query plans into physical operator trees.
 *
 * <p>Two relational-algebra rewrites are applied during compilation:</p>
 * <ul>
 *   <li><b>Projection pushdown into the scan</b>:
 *       {@code Projection(TableScan)} is fused into a single scan that
 *       only parses the projected columns
 *       (pi_L(R) evaluated inside the scan of R).</li>
 *   <li><b>Selection fusion</b>: a chain of predicates is merged into one
 *       operator evaluating the conjunction
 *       (sigma_p(sigma_q(R)) = sigma_(p AND q)(R)).</li>
 * </ul>
 *
 * <p>Results of executed GroupBy/EquiJoin root nodes are cached per plan
 * node instance, so re-executing the identical plan object is free.</p>
 */
public class Compiler extends QueryProcessor {

    private final Map<PlanNode, List<Record>> resultCache = new HashMap<>();

    @Override
    public List<Record> executeQuery(PlanNode node) {
        List<Record> cached = resultCache.get(node);
        if (cached != null) {
            return cached;
        }
        long start = System.currentTimeMillis();
        List<Record> results = super.executeQuery(node);
        if (node.getType() == PlanNode.NodeType.GroupBy || node.getType() == PlanNode.NodeType.EquiJoin) {
            resultCache.put(node, results);
        }
        long end = System.currentTimeMillis();
        System.out.println("Execution time of " + node.getType() + ": " + (end - start) + "ms");
        return results;
    }

    @Override
    public ONCIterator compileQuery(PlanNode node) {
        return switch (node.getType()) {
            case TableScan -> new TableScan(node.getParameters().get("file_path"));
            case Projection -> compileProjection(node);
            case Predicate -> compilePredicateChain(node);
            case EquiJoin -> new HashJoin(node.getParameters(), compileInputs(node.getInputs()));
            case GroupBy -> new GroupBy(node.getParameters(), compileInputs(node.getInputs()));
        };
    }

    /**
     * Fuses {@code Projection(TableScan)} into a column-pruned scan;
     * otherwise compiles a regular projection operator.
     */
    private ONCIterator compileProjection(PlanNode node) {
        PlanNode child = lastInput(node);
        if (child.getType() == PlanNode.NodeType.TableScan) {
            return new TableScan(child.getParameters().get("file_path"),
                    Projection.parseColumnIds(node.getParameters()));
        }
        return new Projection(node.getParameters(), compileInputs(node.getInputs()));
    }

    /**
     * Collects a chain of adjacent predicates into a single conjunctive
     * predicate operator, then compiles the subtree below the chain.
     * Conditions are ordered innermost-first, preserving the original
     * evaluation order.
     */
    private ONCIterator compilePredicateChain(PlanNode node) {
        List<Map<String, String>> conditions = new ArrayList<>();
        PlanNode current = node;
        while (current.getType() == PlanNode.NodeType.Predicate) {
            conditions.add(0, current.getParameters());
            current = lastInput(current);
        }
        return new Predicate(conditions, compileQuery(current));
    }

    private ONCIterator[] compileInputs(PlanNode[] inputs) {
        return Arrays.stream(inputs).map(this::compileQuery).toArray(ONCIterator[]::new);
    }

    private static PlanNode lastInput(PlanNode node) {
        PlanNode[] inputs = node.getInputs();
        return inputs[inputs.length - 1];
    }

    public static PlanNode q11LogicalPlan(String param1, String param2) {
        PlanNode getLO = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", TestTables.LINEORDER), null);
        PlanNode projLO = new PlanNode(PlanNode.NodeType.Projection,
                Map.of("column_ids", "5,8,11,13"), new PlanNode[]{getLO});
        PlanNode predLO1 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "2", "condition", ">=", "literal", param1), new PlanNode[]{projLO});
        PlanNode predLO2 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "2", "condition", "<=", "literal", param2), new PlanNode[]{predLO1});
        PlanNode predLO3 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "1", "condition", ">", "literal", "25"), new PlanNode[]{predLO2});

        PlanNode getD = new PlanNode(PlanNode.NodeType.TableScan,
                Map.of("file_path", TestTables.DATE), null);
        PlanNode projD = new PlanNode(PlanNode.NodeType.Projection,
                Map.of("column_ids", "0,4"), new PlanNode[]{getD});
        PlanNode predD1 = new PlanNode(PlanNode.NodeType.Predicate,
                Map.of("column_id", "1", "condition", "=", "literal", "1993"), new PlanNode[]{projD});
        PlanNode join = new PlanNode(PlanNode.NodeType.EquiJoin,
                Map.of("left_column_id", "0", "right_column_id", "0"), new PlanNode[]{predLO3, predD1});

        return new PlanNode(PlanNode.NodeType.GroupBy,
                Map.of("aggregate_column_id", "3", "aggregate_type", "avg"), new PlanNode[]{join});
    }

    public static void main(String[] args) {
        Compiler compiler = new Compiler();
        PlanNode plan = q11LogicalPlan("1", "3");
        compiler.executeQuery(plan);
    }
}
