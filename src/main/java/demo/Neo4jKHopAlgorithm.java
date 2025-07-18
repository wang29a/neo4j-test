package demo;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
// import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class Neo4jKHopAlgorithm {
    private final Driver driver;
    private final ExecutorService executorService;
    private final int threadPoolSize;
    // private final ObjectMapper objectMapper;
    
    public Neo4jKHopAlgorithm(String uri, String username, String password, int threadPoolSize) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        this.threadPoolSize = threadPoolSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        // this.objectMapper = new ObjectMapper();
    }
    
    /**
     * 生成获取目标节点的Cypher查询
     */
    private String generateCypherGetDst(long src) {
        return String.format(
            "MATCH (v:Vertex {id: %d})-[r:Edge]->(dst:Vertex) RETURN dst.id as dst", 
            src
        );
    }

    /**
     * 生成带属性过滤的获取目标节点的Cypher查询
     */
    private String generateCypherGetDstWithFilter(long src, int property) {
        return String.format(
            "MATCH (v:Vertex {id: %d})-[r:Edge]->(dst:Vertex) RETURN dst.id as dst, r.f%d as rf", 
            src, property
        );
    }
    
    /**
     * 执行Cypher查询并返回结果
     */
    private List<Long> executeCypherQuery(Session session, String cypher) {
        List<Long> destinations = new ArrayList<>();
        
        try {
            Result result = session.run(cypher);
            while (result.hasNext()) {
                Record record = result.next();
                if (record.get("dst") != null && !record.get("dst").isNull()) {
                    destinations.add(record.get("dst").asLong());
                }
            }
        } catch (Neo4jException e) {
            System.err.println("Cypher查询执行失败: " + e.getMessage());
        }
        
        return destinations;
    }

    /**
     * 执行带属性过滤的Cypher查询并返回结果
     */
    private List<FilteredDestination> executeCypherQueryWithFilter(Session session, String cypher) {
        List<FilteredDestination> destinations = new ArrayList<>();
        
        try {
            Result result = session.run(cypher);
            while (result.hasNext()) {
                Record record = result.next();
                if (record.get("dst") != null && !record.get("dst").isNull()) {
                    long dst = record.get("dst").asLong();
                    String weight = record.get("rf") != null && !record.get("rf").isNull() 
                        ? record.get("rf").asString() : "0";
                    destinations.add(new FilteredDestination(dst, weight));
                }
            }
        } catch (Neo4jException e) {
            System.err.println("Cypher查询执行失败: " + e.getMessage());
        }
        
        return destinations;
    }
    
    /**
     * K-Hop递归算法实现
     */
    public long kHopRecursive(int k, int vertexNum, int threadsNum) {
        AtomicLong sum = new AtomicLong(0);
        
        // 定义递归函数
        Function<KHopParams, Void> kHopFunction = new Function<KHopParams, Void>() {
            @Override
            public Void apply(KHopParams params) {
                try (Session session = driver.session()) {
                    String cypher = generateCypherGetDst(params.src);
                    List<Long> destinations = executeCypherQuery(session, cypher);
                    
                    if (destinations.isEmpty()) {
                        return null;
                    }
                    
                    for (Long dst : destinations) {
                        sum.addAndGet(dst);
                        if (params.k > 0) {
                            // 递归调用
                            this.apply(new KHopParams(dst, params.k - 1));
                        }
                    }
                }
                return null;
            }
        };
        
        // 计算采样间隔
        long nsrc = vertexNum / 10000; // 从最大节点数中均匀取出10000个
        if (nsrc == 0) nsrc = 1;
        
        // 创建任务列表
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        // 并行执行K-Hop算法
        for (long vI = 0; vI < vertexNum; vI += nsrc) {
            final long src = vI;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                kHopFunction.apply(new KHopParams(src, k - 1));
            }, executorService);
            futures.add(future);
        }
        
        // 等待所有任务完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        long result = sum.get();
        System.out.println("khop recur sum=" + result);
        return result;
    }

    /**
     * 带属性过滤的K-Hop递归算法实现
     */
    public long kHopRecursiveWithFilter(int k, int vertexNum, int threadsNum, 
                                      int property, long bound) {
        AtomicLong sum = new AtomicLong(0);
        
        // 定义递归函数
        Function<KHopParams, Void> kHopFunction = new Function<KHopParams, Void>() {
            @Override
            public Void apply(KHopParams params) {
                try (Session session = driver.session()) {
                    String cypher = generateCypherGetDstWithFilter(params.src, property);
                    List<FilteredDestination> destinations = executeCypherQueryWithFilter(session, cypher);
                    
                    if (destinations.isEmpty()) {
                        return null;
                    }
                    
                    for (FilteredDestination dest : destinations) {
                        try {
                            // 解析权重并进行过滤
                            long weight = Long.parseLong(dest.weight);
                            if (weight < bound) {
                                continue; // 跳过不满足条件的边
                            }
                            
                            sum.addAndGet(dest.dst);
                            if (params.k > 0) {
                                // 递归调用
                                this.apply(new KHopParams(dest.dst, params.k - 1));
                            }
                        } catch (NumberFormatException e) {
                            // 如果权重不是数字，跳过这条边
                            System.err.println("权重解析失败: " + dest.weight);
                            continue;
                        }
                    }
                }
                return null;
            }
        };
        
        // 计算采样间隔
        long nsrc = vertexNum / 10000;
        if (nsrc == 0) nsrc = 1;
        
        // 创建任务列表
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        // 并行执行K-Hop算法
        for (long vI = 0; vI < vertexNum; vI += nsrc) {
            final long src = vI;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                kHopFunction.apply(new KHopParams(src, k - 1));
            }, executorService);
            futures.add(future);
        }
        
        // 等待所有任务完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        long result = sum.get();
        System.out.println("khop filtered recur sum=" + result + " (property=" + property + ", bound=" + bound + ")");
        return result;
    }
    
    
    /**
     * 获取顶点总数
     */
    public long getVertexCount() {
        try (Session session = driver.session()) {
            Result result = session.run("MATCH (v:Vertex) RETURN count(v) as count");
            if (result.hasNext()) {
                return result.next().get("count").asLong();
            }
        } catch (Neo4jException e) {
            System.err.println("获取顶点数失败: " + e.getMessage());
        }
        return 0;
    }
    
    /**
     * 获取指定顶点的所有邻居
     */
    public Set<Long> getNeighbors(long vertexId) {
        Set<Long> neighbors = new HashSet<>();
        
        try (Session session = driver.session()) {
            String cypher = generateCypherGetDst(vertexId);
            List<Long> destinations = executeCypherQuery(session, cypher);
            neighbors.addAll(destinations);
        }
        
        return neighbors;
    }
    
    
    /**
     * 关闭资源
     */
    public void close() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        driver.close();
    }
    
    /**
     * K-Hop参数类
     */
    private static class KHopParams {
        final long src;
        final int k;
        
        KHopParams(long src, int k) {
            this.src = src;
            this.k = k;
        }
    }

    /**
     * 过滤后的目标节点类
     */
    private static class FilteredDestination {
        final long dst;
        final String weight;
        
        FilteredDestination(long dst, String weight) {
            this.dst = dst;
            this.weight = weight;
        }
    }
    
    /**
     * 示例用法和测试
     */
    public static void main(String[] args) {
        // Neo4j连接配置
        String uri = "bolt://localhost:7688";
        String username = "neo4j";
        String password = "password";
        int threadPoolSize = 16;
        
        Neo4jKHopAlgorithm kHopAlgorithm = new Neo4jKHopAlgorithm(uri, username, password, threadPoolSize);
        
        try {
            // 获取图中顶点总数
            long vertexCount = kHopAlgorithm.getVertexCount();
            System.out.println("图中顶点总数: " + vertexCount);
            
            if (vertexCount == 0) {
                System.out.println("图中没有顶点，请先加载数据");
                return;
            }
            
            // 测试不同的K值
            int[] kValues = {1, 2, 3};
            int threadsNum = 16;
            
            for (int k : kValues) {
                System.out.println("\n=== K-Hop算法测试 (K=" + k + ") ===");
                
                // 测试递归版本
                long startTime = System.currentTimeMillis();
                long recursiveResult = kHopAlgorithm.kHopRecursive(k, (int) vertexCount, threadsNum);
                long recursiveTime = System.currentTimeMillis() - startTime;
                System.out.println("递归版本结果: " + recursiveResult + ", 耗时: " + recursiveTime + "ms");
                
            }
            
            // 测试单个顶点的邻居查询
            System.out.println("\n=== 邻居查询测试 ===");
            Set<Long> neighbors = kHopAlgorithm.getNeighbors(0L);
            System.out.println("顶点0的邻居: " + neighbors);
            
        } catch (Exception e) {
            System.err.println("程序执行出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            kHopAlgorithm.close();
        }
    }
}