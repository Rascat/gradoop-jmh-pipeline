package org.rascat;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.WeaklyConnectedComponentsAsCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;


public class Main {

    /**
     * Runs the program on the example data graph.
     *
     * The example provides an overview over the possible usage of gradoop operators in general.
     * Documentation for all available operators as well as their detailed description can be
     * found in the projects wiki.
     *
     * Using a prepared graph (see link below), the program will:
     * <ol>
     *   <li>create the graph based on the given gdl string</li>
     *   <li>show input graphs</li>
     *   <li>calculate and show the overlap</li>
     *   <li>calculate and show the combination result (combined with subgraph operator)</li>
     *   <li>calculate and show the WCC result</li>
     * </ol>
     *
     * @param args no args used
     * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Getting-started">
     * Gradoop Quickstart Example</a>
     * @throws Exception on failure
     */
    public static void main(String[] args) throws Exception {
        // create flink execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create loader
        FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

        // load data
        loader.initDatabaseFromString(getGraphGDLString());

        // show input
        LogicalGraph graph1 = loader.getLogicalGraphByVariable("g1");

        System.out.println("INPUT_GRAPH_1");
        graph1.print();

        LogicalGraph graph2 = loader.getLogicalGraphByVariable("g2");

        System.out.println("INPUT_GRAPH_2");
        graph2.print();

        // execute overlap
        LogicalGraph overlap = graph2.overlap(graph1);

        System.out.println("OVERLAP_GRAPH");
        overlap.print();

        // execute combine
        LogicalGraph workGraph = graph1.combine(graph2)
                .subgraph(
                        v -> true,
                        e -> e.getLabel().equals("worksAt"));

        System.out.println("COMBINED_GRAPH with SUBGRAPH");
        workGraph.print();

        // execute WCC
        GraphCollection workspaces = new WeaklyConnectedComponentsAsCollection(5).execute(workGraph);

        System.out.println("CONNECTED_COMPONENTS_GRAPH");
        workspaces.print();
    }

    /**
     * Example Graph for QuickstartExample
     *
     * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Getting-started">
     * Gradoop Quickstart Example</a>
     * @return example graph
     */
    public static String getGraphGDLString() {

        return
                "g1:graph[" +
                        "      (p1:Person {name: \"Bob\", age: 24})-[:friendsWith]->" +
                        "      (p2:Person{name: \"Alice\", age: 30})-[:friendsWith]->(p1)" +
                        "      (p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27})-[:friendsWith]->(p2)" +
                        "      (p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40})-[:friendsWith]->(p3)" +
                        "      (p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33})-[:friendsWith]->(p4)" +
                        "      (c1:Company {name: \"Acme Corp\"})" +
                        "      (c2:Company {name: \"Globex Inc.\"})" +
                        "      (p2)-[:worksAt]->(c1)" +
                        "      (p4)-[:worksAt]->(c1)" +
                        "      (p5)-[:worksAt]->(c1)" +
                        "      (p1)-[:worksAt]->(c2)" +
                        "      (p3)-[:worksAt]->(c2)" +
                        "      ]" +
                        "      g2:graph[" +
                        "      (p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37})-[:friendsWith]->(p4)" +
                        "      (p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23})-[:friendsWith]->(p6)" +
                        "      (p8:Person {name: \"Jil\", age: 32})-[:friendsWith]->(p7)-[:friendsWith]->(p8)" +
                        "      (p6)-[:worksAt]->(c2)" +
                        "      (p7)-[:worksAt]->(c2)" +
                        "      (p8)-[:worksAt]->(c1)" +
                        "]";
    }
}
