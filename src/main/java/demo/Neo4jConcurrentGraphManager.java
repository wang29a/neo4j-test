package demo;

import org.neo4j.bolt.connection.values.Relationship;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Neo4jConcurrentGraphManager {
    private final Driver driver;
    private final ExecutorService executorService;
    private final int threadPoolSize;
    private final AtomicInteger vertexCounter = new AtomicInteger(0);
    private final AtomicInteger edgeCounter = new AtomicInteger(0);
    
    public Neo4jConcurrentGraphManager(String uri, String username, String password, int threadPoolSize) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        this.threadPoolSize = threadPoolSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        
        // 创建索引以提高查询性能
        createIndexes();
    }
    
    private void createIndexes() {
        try (Session session = driver.session()) {
            // 为Vertex节点的id属性创建索引
            session.run("CREATE INDEX vertex_id_index IF NOT EXISTS FOR (v:Vertex) ON (v.id)");
            System.out.println("索引创建完成");
        } catch (Exception e) {
            System.err.println("创建索引时出错: " + e.getMessage());
        }
    }
    
    // 并发插入点
    public CompletableFuture<Void> insertVertexAsync(Integer vertexId) {
        return CompletableFuture.runAsync(() -> {
            try (Session session = driver.session()) {
                session.run("MERGE (v:Vertex {id: $id})", 
                           Values.parameters("id", vertexId));
                vertexCounter.incrementAndGet();
                System.out.println("插入点: " + vertexId + " (线程: " + Thread.currentThread().getName() + ")");
            } catch (Neo4jException e) {
                System.err.println("插入点失败 " + vertexId + ": " + e.getMessage());
            }
        }, executorService);
    }

    // 根据源顶点和目标顶点ID生成边属性值
    private String generateEdgeAttribute(int src, int dst) {
        int dig = (src + dst) % 100000;
        return String.format("%05d", dig); // 格式化为5位数字，不足补0
    }
    
    // 并发插入边
    public CompletableFuture<Void> insertEdgeAsync(int fromId, int toId) {
        return CompletableFuture.runAsync(() -> {
            try (Session session = driver.session()) {
                // 使用事务确保数据一致性
                // 生成边的6个属性值
                String f1 = generateEdgeAttribute(fromId, toId);
                String f2 = f1+f1+f1+f1;
                session.writeTransaction(tx -> {
                    // 确保起始点和结束点存在
                    tx.run("MERGE (from:Vertex {id: $fromId}) MERGE (to:Vertex {id: $toId})", 
                           Values.parameters("fromId", fromId, "toId", toId));
                    
                    // 创建边
                    tx.run("MATCH (from:Vertex {id: $fromId}), (to:Vertex {id: $toId}) " +
                           "CREATE (from)-[r:Edge {f1: $f1, f2: $f2, f3: $f3, f4: $f4, f5: $f5, f6: $f6, f7: $f7, f8: $f8}]->(to)",
                           Values.parameters("fromId", fromId, "toId", toId, 
                                           "f1", f1, "f2", f2, "f3", f2, "f4", f2, "f5", f2, "f6", f2, "f7", f2, "f8", f2));
                    return null;
                });
                
                edgeCounter.incrementAndGet();
                System.out.println("插入边: " + fromId + " -> " + toId + " (线程: " + Thread.currentThread().getName() + ")");
            } catch (Neo4jException e) {
                System.err.println("插入边失败 " + fromId + " -> " + toId + ": " + e.getMessage());
            }
        }, executorService);
    }
    
    // 批量插入点
    public CompletableFuture<Void> insertVerticesBatch(List<Integer> vertexIds) {
        return CompletableFuture.runAsync(() -> {
            try (Session session = driver.session()) {
                session.writeTransaction(tx -> {
                    for (Integer vertexId : vertexIds) {
                        tx.run("MERGE (v:Vertex {id: $id})", Values.parameters("id", vertexId));
                        vertexCounter.incrementAndGet();
                    }
                    return null;
                });
                System.out.println("批量插入点完成，数量: " + vertexIds.size() + " (线程: " + Thread.currentThread().getName() + ")");
            } catch (Neo4jException e) {
                System.err.println("批量插入点失败: " + e.getMessage());
            }
        }, executorService);
    }
    
    // 从文件读取图数据并插入
    public void loadGraphFromFile(String filePath) {
        Set<Integer> vertices = new HashSet<>();
        List<int[]> edges = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                
                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    int fromId = Integer.parseInt(parts[0]);
                    int toId = Integer.parseInt(parts[1]);
                    
                    // 收集所有顶点
                    vertices.add(fromId);
                    vertices.add(toId);
                    
                    // 异步插入边
                    edges.add(new int[]{fromId, toId});
                }
            }
            
        } catch (IOException e) {
            System.err.println("读取文件失败: " + e.getMessage());
            return;
        }

        System.out.println("文件读取完成，顶点数: " + vertices.size() + ", 边数: " + edges.size());

        // 批量插入顶点
        System.out.println("开始并发插入顶点...");
        List<Integer> vertexList = new ArrayList<>(vertices);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int batchSize = 10000;
        for (int i = 0; i < vertexList.size(); i += batchSize) {
            int end = Math.min(i + batchSize, vertexList.size());
            List<Integer> batch = vertexList.subList(i, end);
            CompletableFuture<Void> vertexFuture = insertVerticesBatch(batch);
            futures.add(vertexFuture);
        }
            
        // 等待所有操作完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        System.out.println("顶点插入完成！");

        // 并发插入所有边
        System.out.println("开始并发插入边...");
        List<CompletableFuture<Void>> edgeFutures = new ArrayList<>();
        
        for (int[] edge : edges) {
            int fromId = edge[0];
            int toId = edge[1];
            CompletableFuture<Void> edgeFuture = insertEdgeAsync(fromId, toId);
            edgeFutures.add(edgeFuture);
        }
        
        // 等待所有边插入完成
        CompletableFuture.allOf(edgeFutures.toArray(new CompletableFuture[0])).join();
        
        System.out.println("图数据加载完成！");
        System.out.println("总点数: " + vertexCounter.get());
        System.out.println("总边数: " + edgeCounter.get());
            
    }
    
    // 获取某个点的出边
    public List<EdgeInfo> getVertexOutgoingEdges(int vertexId) {
        List<EdgeInfo> edges = new ArrayList<>();
        
        try (Session session = driver.session()) {
            Result result = session.run(
                "MATCH (v:Vertex {id: $vertexId})-[r:Edge]->(other:Vertex) " +
                "RETURN r, other.id as otherId",
                Values.parameters("vertexId", vertexId)
            );
            
            while (result.hasNext()) {
                Record record = result.next();
                Relationship edge = (Relationship) record.get("r").asRelationship();
                String otherId = record.get("otherId").asString();
                
                EdgeInfo edgeInfo = new EdgeInfo(
                    otherId,
                    "outgoing",
                    ((Record) edge).get("f1").asString(),
                    ((Record) edge).get("f2").asString(),
                    ((Record) edge).get("f3").asString(),
                    ((Record) edge).get("f4").asString(),
                    ((Record) edge).get("f5").asString(),
                    ((Record) edge).get("f6").asString(),
                    ((Record) edge).get("f7").asString(),
                    ((Record) edge).get("f8").asString()
                );
                
                edges.add(edgeInfo);
            }
        } catch (Neo4jException e) {
            System.err.println("查询顶点出边失败: " + e.getMessage());
        }
        
        return edges;
    }
    
    
    // 清空数据库
    public void clearDatabase() {
        try (Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
            vertexCounter.set(0);
            edgeCounter.set(0);
            System.out.println("数据库已清空");
        } catch (Neo4jException e) {
            System.err.println("清空数据库失败: " + e.getMessage());
        }
    }
    
    // 关闭资源
    public void close() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
        driver.close();
    }
    
    // 边信息类
    public static class EdgeInfo {
        private final String connectedVertexId;
        private final String direction;
        private final String f1, f2, f3, f4, f5, f6, f7, f8;
        
        public EdgeInfo(String connectedVertexId, 
            String direction, String f1, String f2, String f3,
            String f4, String f5, String f6, String f7, String f8) {
            this.connectedVertexId = connectedVertexId;
            this.direction = direction;
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
            this.f5 = f5;
            this.f6 = f6;
            this.f7 = f7;
            this.f8 = f8;
        }
        
        // Getters
        public String getConnectedVertexId() { return connectedVertexId; }
        public String getDirection() { return direction; }
        public String getF1() { return f1; }
        public String getF2() { return f2; }
        public String getF3() { return f3; }
        public String getF4() { return f4; }
        public String getF5() { return f5; }
        public String getF6() { return f6; }
        public String getF7() { return f7; }
        public String getF8() { return f8; }
        
        @Override
        public String toString() {
            return String.format("EdgeInfo{connectedVertex='%s', direction='%s', f1='%s', f2='%s', f3='%s', f4='%s', f5='%s', f6='%s'}", 
                               connectedVertexId, direction, f1, f2, f3, f4, f5, f6);
        }
    }
    
    // 示例用法
    public static void main(String[] args) {
        // Neo4j连接配置
        String uri = "bolt://localhost:7687";
        String username = "neo4j";
        String password = "password";
        int threadPoolSize = 10;
        
        Neo4jConcurrentGraphManager manager = new Neo4jConcurrentGraphManager(uri, username, password, threadPoolSize);
        
        try {
            // 清空数据库
            manager.clearDatabase();
            
            // 示例1: 手动插入点和边
            System.out.println("=== 示例1: 手动插入点和边 ===");
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            // 插入点
            futures.add(manager.insertVertexAsync(0));
            futures.add(manager.insertVertexAsync(1));
            futures.add(manager.insertVertexAsync(2));
            
            // 插入边
            // futures.add(manager.insertEdgeAsync("A", "B", "val1", "val2", "val3", "val4", "val5", "val6"));
            // futures.add(manager.insertEdgeAsync("B", "C", "val7", "val8", "val9", "val10", "val11", "val12"));
            // futures.add(manager.insertEdgeAsync("A", "C", "val13", "val14", "val15", "val16", "val17", "val18"));
            
            // 等待所有操作完成
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            
            // 示例2: 从文件加载图数据
            System.out.println("\n=== 示例2: 从文件加载图数据 ===");
            // 假设有graph.txt文件，内容如下：
            // 0 1
            // 1 2
            // 0 2
            // 可以取消注释以下行来测试文件加载
            // manager.loadGraphFromFile("graph.txt");
            
            // 示例3: 查询某个点的边
            System.out.println("\n=== 示例3: 查询点的边 ===");
            Thread.sleep(1000); // 等待插入完成
            
            // 查询点0的出边
            List<EdgeInfo> aOutEdges = manager.getVertexOutgoingEdges(0);
            System.out.println("点0的出边:");
            for (EdgeInfo edge : aOutEdges) {
                System.out.println("  " + edge);
            }
            
            
        } catch (InterruptedException e) {
            System.err.println("程序被中断: " + e.getMessage());
        } finally {
            manager.close();
        }
    }
}