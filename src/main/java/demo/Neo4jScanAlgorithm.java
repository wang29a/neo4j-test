package demo;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class Neo4jScanAlgorithm {
    private final Driver driver;
    private final ExecutorService executorService;
    private final int threadPoolSize;
    
    public Neo4jScanAlgorithm(String uri, String username, String password, int threadPoolSize) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        this.threadPoolSize = threadPoolSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }
    /**
     * 生成带属性过滤的获取目标节点的Cypher查询
     */
    private String generateCypherGetDst(long src, int property) {
        return String.format(
            "MATCH (v:Vertex {id: %d})-[r:Edge]->(dst:Vertex) RETURN r.f%d as rf", 
            src, property
        );
    }

    private List<String> executeCypherQuery(Session session, String cypher) {
        List<String> property = new ArrayList<>();
        
        try {
            Result result = session.run(cypher);
            while (result.hasNext()) {
                Record record = result.next();
                String weight = record.get("rf") != null && !record.get("rf").isNull() 
                    ? record.get("rf").asString() : "0";
                property.add(weight);
            }
        } catch (Neo4jException e) {
            System.err.println("Cypher查询执行失败: " + e.getMessage());
        }
        
        return property;
    }

    /**
     * scan 算法实现
     */
    public long scan(long vertexNum, int threadsNum, int sub_property) {
        AtomicLong sum = new AtomicLong(0);
        
        // 定义递归函数
        Function<Params, Void> scanFunction = new Function<Params, Void>() {
            @Override
            public Void apply(Params params) {
                try (Session session = driver.session()) {
                    String cypher = generateCypherGetDst(params.src, params.sub_property);
                    List<String> property = executeCypherQuery(session, cypher);
                    
                    if (property.isEmpty()) {
                        return null;
                    }
                    for (String pro : property) {
                        sum.addAndGet(Long.parseLong(pro));
                    }
                }
                return null;
            }
        };
        
        
        // 创建任务列表
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        // 并行执行K-Hop算法
        for (long vI = 0; vI < vertexNum; vI ++) {
            final long src = vI;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                scanFunction.apply(new Params(src, sub_property));
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
     * scan参数类
     */
    private static class Params {
        final long src;
        final int sub_property;
        
        Params(long src, int sub_property) {
            this.src = src;
            this.sub_property = sub_property;
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
        
        Neo4jScanAlgorithm scanAlgorithm = new Neo4jScanAlgorithm(uri, username, password, threadPoolSize);
        
        try {
            
            long vertexCount = 10;
            int threadsNum = 16;
            
            System.out.println("\n=== Scan 算法测试 ===");
            
            long startTime = System.currentTimeMillis();
            long Result = scanAlgorithm.scan(vertexCount, threadsNum, 0);
            long Time = System.currentTimeMillis() - startTime;
            System.out.println("递归版本结果: " + Result + ", 耗时: " + Time + "ms");
            
        } catch (Exception e) {
            System.err.println("程序执行出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanAlgorithm.close();
        }
    }
}
