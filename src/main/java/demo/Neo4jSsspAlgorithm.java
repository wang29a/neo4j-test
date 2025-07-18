package demo;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class Neo4jSsspAlgorithm {
    private final Driver driver;
    private final ExecutorService executorService;
    private final int threadPoolSize;
    
    public Neo4jSsspAlgorithm(String uri, String username, String password, int threadPoolSize) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        this.threadPoolSize = threadPoolSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }
    
    private String generateCypherGetDstForSSSP(long src, int property) {
        return String.format(
            "MATCH (v:Vertex {id: %d})-[r:Edge]->(dst:Vertex) RETURN dst.id as dst, r.f%d as rf", 
            src, property
        );
    }

    /**
     * 执行SSSP算法的Cypher查询，返回邻接节点和边权重
     */
    private List<EdgeInfo> executeCypherQueryForSSSP(Session session, String cypher) {
        List<EdgeInfo> edges = new ArrayList<>();
        
        try {
            Result result = session.run(cypher);
            while (result.hasNext()) {
                Record record = result.next();
                long dst = record.get("dst") != null && !record.get("dst").isNull() 
                    ? record.get("dst").asLong() : -1;
                String weightStr = record.get("rf") != null && !record.get("rf").isNull() 
                    ? record.get("rf").asString() : "0";
                
                if (dst != -1) {
                    long weight = Long.parseLong(weightStr);
                    edges.add(new EdgeInfo(dst, weight));
                }
            }
        } catch (Neo4jException e) {
            System.err.println("SSSP Cypher查询执行失败: " + e.getMessage());
        }
        
        return edges;
    }
    /**
     * SSSP (Single Source Shortest Path) 算法实现
     */
    public long sssp(long vertexNum, int threadNum, int subPropertyId, long root) {
        final long DEFAULT_VALUE = Long.MAX_VALUE;
        final int basicChunk = 64;
        
        System.out.println("source=" + root);
        
        // 距离数组
        final long[] values = new long[(int)vertexNum];
        Arrays.fill(values, DEFAULT_VALUE);
        values[(int)root] = 0;
        
        // 活跃节点位图 - 使用数组来避免final问题
        final BitSet[] activeBitmaps = new BitSet[2];
        activeBitmaps[0] = new BitSet((int)vertexNum);
        activeBitmaps[1] = new BitSet((int)vertexNum);
        activeBitmaps[0].set((int)root);
        
        // AtomicInteger activated = new AtomicInteger(1);
        final AtomicInteger currentBitmapIndex = new AtomicInteger(0);
        AtomicInteger activated = new AtomicInteger(1);
        
        int step = 0;
        while (activated.get() > 0) {
            System.out.println("step=" + (++step) + " activated=" + activated.get());
            activated.set(0);
            
            final int currentIdx = currentBitmapIndex.get();
            final int nextIdx = 1 - currentIdx;
            final BitSet activeIn = activeBitmaps[currentIdx];
            final BitSet activeOut = activeBitmaps[nextIdx];
            
            activeOut.clear();
            
            if (step > vertexNum) {
                System.err.println("SSSP算法可能陷入死循环，步数超过节点数");
                break;
            }
            
            // 并行处理活跃节点
            List<CompletableFuture<Integer>> futures = new ArrayList<>();
            
            for (int beginVi = 0; beginVi < vertexNum; beginVi += basicChunk) {
                final int startIndex = beginVi;
                final int endIndex = Math.min(beginVi + basicChunk, (int)vertexNum);
                
                CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
                    int localActivated = 0;
                    
                    for (int vi = startIndex; vi < endIndex; vi++) {
                        if (activeIn.get(vi)) {
                            // 松弛操作
                            long srcValue = values[vi];
                            
                            try (Session session = driver.session()) {
                                String cypher = generateCypherGetDstForSSSP(vi, subPropertyId);
                                List<EdgeInfo> edges = executeCypherQueryForSSSP(session, cypher);
                                
                                for (EdgeInfo edge : edges) {
                                    long dst = edge.dst;
                                    long weight = edge.weight;
                                    long relaxDist = srcValue + weight;
                                    
                                    if (relaxDist < values[(int)dst]) {
                                        synchronized (values) {
                                            if (relaxDist < values[(int)dst]) {
                                                values[(int)dst] = relaxDist;
                                                activeOut.set((int)dst);
                                                localActivated++;
                                            }
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                System.err.println("SSSP松弛操作失败: " + e.getMessage());
                            }
                        }
                    }
                    
                    return localActivated;
                }, executorService);
                
                futures.add(future);
            }
            
            // 等待所有任务完成并累加结果
            for (CompletableFuture<Integer> future : futures) {
                try {
                    activated.addAndGet(future.get());
                } catch (Exception e) {
                    System.err.println("SSSP并行任务执行失败: " + e.getMessage());
                }
            }
            
            // 交换活跃节点位图索引
            currentBitmapIndex.set(nextIdx);
        }
        
        // 计算最终结果
        AtomicLong sum = new AtomicLong(0);
        List<CompletableFuture<Void>> sumFutures = new ArrayList<>();
        
        for (int i = 0; i < vertexNum; i += basicChunk) {
            final int startIndex = i;
            final int endIndex = Math.min(i + basicChunk, (int)vertexNum);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                long localSum = 0;
                for (int j = startIndex; j < endIndex; j++) {
                    if (values[j] != DEFAULT_VALUE) {
                        localSum += values[j];
                    }
                }
                sum.addAndGet(localSum);
            }, executorService);
            
            sumFutures.add(future);
        }
        
        // 等待所有求和任务完成
        CompletableFuture.allOf(sumFutures.toArray(new CompletableFuture[0])).join();
        
        long result = sum.get();
        System.out.println("sssp sum=" + result);
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
     * 边信息类，用于SSSP算法
     */
    private static class EdgeInfo {
        final long dst;
        final long weight;
        
        EdgeInfo(long dst, long weight) {
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
        
        Neo4jSsspAlgorithm ssspAlgorithm = new Neo4jSsspAlgorithm(uri, username, password, threadPoolSize);
        
        try {
            long vertexCount = 10;
            int threadsNum = 16;
            long root = 0;
            
            
            System.out.println("\n=== SSSP 算法测试 ===");
            long startTime = System.currentTimeMillis();
            long ssspResult = ssspAlgorithm.sssp(vertexCount, threadsNum, 0, root);
            long ssspTime = System.currentTimeMillis() - startTime;
            System.out.println("SSSP算法结果: " + ssspResult + ", 耗时: " + ssspTime + "ms");
            
        } catch (Exception e) {
            System.err.println("程序执行出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            ssspAlgorithm.close();
        }
    }
}
