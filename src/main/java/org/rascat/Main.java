package org.rascat;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.WeaklyConnectedComponentsAsCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;


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
     * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Getting-started">
     * Gradoop Quickstart Example</a>
     * @throws Exception on failure
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void runQuickstart() throws Exception {
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

    /*
     * ============================== HOW TO RUN THIS BENCHMARK: =================
     *
     * You are expected to see the different run modes for the same benchmark.
     * Note the units are different, scores are consistent with each other.
     *
     * You can run this test:
     *
     * a) Via the command line:
     *    $ mvn clean install
     *    $ java -jar target/gradoop-pipeline-1.0-SNAPSHOT.jar org.rascat.Master -f 1
     *    (we requested a single fork; there are also other options, see -h)
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Main.class.getSimpleName())
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .build();

        new Runner(opt).run();
    }

    /**
     * Example Graph for QuickstartExample
     *
     * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Getting-started">
     * Gradoop Quickstart Example</a>
     * @return example graph
     */
    private static String getGraphGDLString() {

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
