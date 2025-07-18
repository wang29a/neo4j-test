package demo;

import org.neo4j.bolt.connection.values.Relationship;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
    List<long[]> edges = new ArrayList<>();

    private Long maxVid;
    
    public Neo4jConcurrentGraphManager(String uri, String username, String password, int threadPoolSize) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        this.threadPoolSize = threadPoolSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        this.maxVid = null;
        
        // 创建索引以提高查询性能
        // createIndexes();
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

    // 获取文件大小
    private long getFileSize(String filename) throws IOException {
        File file = new File(filename);
        if (!file.exists()) {
            throw new FileNotFoundException("文件不存在: " + filename);
        }
        return file.length();
    }

    // 从二进制文件读取图数据并插入
    public void loadGraphFromBinaryFile(String inputFilename) {
        System.out.println("开始从二进制文件读取图数据: " + inputFilename);
        
        Set<Long> vertices = new HashSet<>();
        
        try {
            long fileSize = getFileSize(inputFilename);
            long edgeNum = fileSize / 2 / Long.BYTES; // 每条边包含两个long值
            
            System.out.println("文件大小: " + fileSize + " 字节");
            System.out.println("预计边数: " + edgeNum);
            
            try (FileInputStream fis = new FileInputStream(inputFilename);
                 FileChannel channel = fis.getChannel()) {
                
                // 设置缓冲区大小 (4MB)
                final int BUFFER_SIZE = 4 * 1024 * 1024;
                final int VERTEX_PAIR_SIZE = 2 * Long.BYTES; // 16字节
                final int BUFFER_PAIRS = BUFFER_SIZE / VERTEX_PAIR_SIZE;
                final int ACTUAL_BUFFER_SIZE = BUFFER_PAIRS * VERTEX_PAIR_SIZE;
                
                ByteBuffer buffer = ByteBuffer.allocate(ACTUAL_BUFFER_SIZE);
                buffer.order(ByteOrder.nativeOrder()); // 使用本地字节序
                
                long offset = 0;
                this.maxVid = (long) 0;
                
                // 预分配边列表容量
                edges = new ArrayList<>((int) Math.min(edgeNum, Integer.MAX_VALUE));
                
                while (offset < fileSize) {
                    buffer.clear();
                    
                    // 读取数据到缓冲区
                    int bytesRead = channel.read(buffer);
                    if (bytesRead == -1) break;
                    
                    buffer.flip(); // 准备读取
                    
                    // 处理缓冲区中的边数据
                    int pairsInBuffer = bytesRead / VERTEX_PAIR_SIZE;
                    for (int i = 0; i < pairsInBuffer; i++) {
                        long u = buffer.getLong();
                        long v = buffer.getLong();
                        
                        // 收集顶点
                        vertices.add(u);
                        vertices.add(v);
                        
                        // 收集边
                        edges.add(new long[]{u, v});
                        
                        // 更新最大顶点ID
                        maxVid = Math.max(maxVid, Math.max(u, v));
                    }
                    
                    offset += bytesRead;
                    
                    // 显示进度
                    if (edges.size() % 100000 == 0) {
                        System.out.println("已读取边数: " + edges.size() + " / " + edgeNum);
                    }
                }
                
                System.out.println("二进制文件读取完成！");
                System.out.println("顶点数: " + vertices.size());
                System.out.println("边数: " + edges.size());
                System.out.println("最大顶点ID: " + maxVid);
                
            }
            
        } catch (IOException e) {
            System.err.println("读取二进制文件失败: " + e.getMessage());
            e.printStackTrace();
            return;
        }
        
        // 批量插入顶点
        System.out.println("开始并发插入顶点...");
        List<Long> vertexList = new ArrayList<>(vertices);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (long i = 0; i < vertexList.size(); i ++) {
            CompletableFuture<Void> vertexFuture = insertVertexAsync(i);
            futures.add(vertexFuture);
        }
        
        // 等待所有顶点插入完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        System.out.println("顶点插入完成！");
        
        // 并发插入所有边
        System.out.println("开始并发插入边...");
        List<CompletableFuture<Void>> edgeFutures = new ArrayList<>();
        
        for (long[] edge : edges) {
            long fromId = edge[0];
            long toId = edge[1];
            CompletableFuture<Void> edgeFuture = insertEdgeAsyncLong(fromId, toId);
            edgeFutures.add(edgeFuture);
        }
        
        // 等待所有边插入完成
        CompletableFuture.allOf(edgeFutures.toArray(new CompletableFuture[0])).join();
        
        System.out.println("二进制图数据加载完成！");
        System.out.println("总点数: " + vertexCounter.get());
        System.out.println("总边数: " + edgeCounter.get());
    }

    // 支持long类型的并发插入边
    public CompletableFuture<Void> insertEdgeAsyncLong(long fromId, long toId) {
        return CompletableFuture.runAsync(() -> {
            try (Session session = driver.session()) {
                // 使用事务确保数据一致性
                // 生成边的8个属性值
                String f1 = generateEdgeAttribute(fromId, toId);
                String f2 = f1 + f1 + f1 + f1;
                
                session.writeTransaction(tx -> {
                    // 确保起始点和结束点存在
                    // tx.run("MERGE (from:Vertex {id: $fromId}) MERGE (to:Vertex {id: $toId})", 
                    //        Values.parameters("fromId", fromId, "toId", toId));
                    
                    // 创建边
                    tx.run("MATCH (from:Vertex {id: $fromId}), (to:Vertex {id: $toId}) " +
                           "CREATE (from)-[r:Edge {f1: $f1, f2: $f2, f3: $f3, f4: $f4, f5: $f5, f6: $f6, f7: $f7, f8: $f8}]->(to)",
                           Values.parameters("fromId", fromId, "toId", toId, 
                                           "f1", f1, "f2", f2, "f3", f2, "f4", f2, "f5", f2, "f6", f2, "f7", f2, "f8", f2));
                    return null;
                });
                
                edgeCounter.incrementAndGet();
                if (edgeCounter.get() % 10000 == 0) {
                    System.out.println("已插入边数: " + edgeCounter.get() + " (线程: " + Thread.currentThread().getName() + ")");
                }
            } catch (Neo4jException e) {
                System.err.println("插入边失败 " + fromId + " -> " + toId + ": " + e.getMessage());
            }
        }, executorService);
    }
    
    // 并发插入点
    public CompletableFuture<Void> insertVertexAsync(Long vertexId) {
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
    private String generateEdgeAttribute(long src, long dst) {
        long dig = (src + dst) % 100000;
        return String.format("%05d", dig); // 格式化为5位数字，不足补0
    }
    
    // 从文件读取图数据并插入
    public void loadGraphFromFile(String filePath) {
        Set<Long> vertices = new HashSet<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                
                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    long fromId = Long.parseLong(parts[0]);
                    long toId = Long.parseLong(parts[1]);
                    
                    // 收集所有顶点
                    vertices.add(fromId);
                    vertices.add(toId);
                    
                    // 异步插入边
                    edges.add(new long[]{fromId, toId});
                }
            }
            
        } catch (IOException e) {
            System.err.println("读取文件失败: " + e.getMessage());
            return;
        }

        System.out.println("文件读取完成，顶点数: " + vertices.size() + ", 边数: " + edges.size());

        // 批量插入顶点
        System.out.println("开始并发插入顶点...");
        List<Long> vertexList = new ArrayList<>(vertices);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (long i = 0; i < vertexList.size(); i ++) {
            CompletableFuture<Void> vertexFuture = insertVertexAsync(i);
            futures.add(vertexFuture);
        }
            
        // 等待所有操作完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        System.out.println("顶点插入完成！");

        // 并发插入所有边
        System.out.println("开始并发插入边...");
        List<CompletableFuture<Void>> edgeFutures = new ArrayList<>();
        
        for (long[] edge : edges) {
            long fromId = edge[0];
            long toId = edge[1];
            CompletableFuture<Void> edgeFuture = insertEdgeAsyncLong(fromId, toId);
            edgeFutures.add(edgeFuture);
        }
        
        // 等待所有边插入完成
        CompletableFuture.allOf(edgeFutures.toArray(new CompletableFuture[0])).join();
        
        System.out.println("图数据加载完成！");
        System.out.println("总点数: " + vertexCounter.get());
        System.out.println("总边数: " + edgeCounter.get());
            
    }
    
    // 获取某个点的出边
    // public List<EdgeInfo> getVertexOutgoingEdges(int vertexId) {
    //     List<EdgeInfo> edges = new ArrayList<>();
        
    //     try (Session session = driver.session()) {
    //         Result result = session.run(
    //             "MATCH (v:Vertex {id: $vertexId})-[r:Edge]->(other:Vertex) " +
    //             "RETURN r, other.id as otherId",
    //             Values.parameters("vertexId", vertexId)
    //         );
            
    //         while (result.hasNext()) {
    //             Record record = result.next();
    //             Value edge = record.get("r");
    //             int otherId = record.get("otherId").asInt();
                
    //             EdgeInfo edgeInfo = new EdgeInfo(
    //                 otherId,
    //                 "outgoing",
    //                 edge.get("f1").asString(),
    //                 edge.get("f2").asString(),
    //                 edge.get("f3").asString(),
    //                 edge.get("f4").asString(),
    //                 edge.get("f5").asString(),
    //                 edge.get("f6").asString(),
    //                 edge.get("f7").asString(),
    //                 edge.get("f8").asString()
    //             );
                
    //             edges.add(edgeInfo);
    //         }
    //     } catch (Neo4jException e) {
    //         System.err.println("查询顶点出边失败: " + e.getMessage());
    //     }
        
    //     return edges;
    // }

    // 并发读
    public CompletableFuture<Void> readEdgeAsync(long src, long dst) {
        return CompletableFuture.runAsync(() -> {
            try (Session session = driver.session()) {
                session.run(
                    "MATCH (v:Vertex {id: $src})-[r:Edge]->(u:Vertex {id: $dst}) " +
                    "RETURN r",
                    Values.parameters("src", src, "dst", dst)
                );
                System.out.println("Edge src:" + src + "dst:" + dst + " (线程: " + Thread.currentThread().getName() + ")");
            } catch (Neo4jException e) {
                System.err.println("读失败 Edge src:" + src + "dst:" + dst + e.getMessage());
            }
        }, executorService);
    }

    // 并发更新
    public CompletableFuture<Void> updateVertexAsync(long src, long dst) {
        return CompletableFuture.runAsync(() -> {
            try (Session session = driver.session()) {
                String f1 = generateEdgeAttribute(src, dst);
                String f2 = f1+f1+f1+f1;
                session.run(
                    "MATCH (v:Vertex {id: $src})-[r:Edge]->(u:Vertex {id: $dst}) " +
                    "SET r.f1 = $f1," +
                    "r.f2 = $f2," +
                    "r.f3 = $f3," +
                    "r.f4 = $f4," +
                    "r.f5 = $f5," +
                    "r.f6 = $f6," +
                    "r.f7 = $f7," +
                    "r.f8 = $f8 " +
                    "RETURN r",
                    Values.parameters("src", src, "dst", dst, "f1", f1, "f2", f2, "f3", f2, "f4", f2, "f5", f2, "f6", f2, "f7", f2, "f8", f2)
                );
                System.out.println("update Edge src:" + src + "dst:" + dst + " (线程: " + Thread.currentThread().getName() + ")");
            } catch (Neo4jException e) {
                System.err.println("更新失败 Edge src:" + src + "dst:" + dst + e.getMessage());
            }
        }, executorService);
    }

    public void read() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        long startTime = System.currentTimeMillis();
        for (long[] edge : edges) {
            long fromId = edge[0];
            long toId = edge[1];
            CompletableFuture<Void> edgeFuture = readEdgeAsync(fromId, toId);
            futures.add(edgeFuture);
        }

        // 等待所有操作完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long Time = System.currentTimeMillis() - startTime;
        System.out.println("read完成 time:" + Time);
    }

    public void update() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        long startTime = System.currentTimeMillis();
        for (long[] edge : edges) {
            long fromId = edge[0];
            long toId = edge[1];
            CompletableFuture<Void> edgeFuture = readEdgeAsync(fromId, toId);
            futures.add(edgeFuture);
        }

        // 等待所有操作完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long Time = System.currentTimeMillis() - startTime;
        System.out.println("update完成 time:" + Time);
    }

    // 获取 Neo4j 驱动器（用于图算法类）
    public Driver getDriver() {
        return driver;
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
    // public static class EdgeInfo {
    //     private final int connectedVertexId;
    //     private final String direction;
    //     private final String f1, f2, f3, f4, f5, f6, f7, f8;
        
    //     public EdgeInfo(int connectedVertexId, 
    //         String direction, String f1, String f2, String f3,
    //         String f4, String f5, String f6, String f7, String f8) {
    //         this.connectedVertexId = connectedVertexId;
    //         this.direction = direction;
    //         this.f1 = f1;
    //         this.f2 = f2;
    //         this.f3 = f3;
    //         this.f4 = f4;
    //         this.f5 = f5;
    //         this.f6 = f6;
    //         this.f7 = f7;
    //         this.f8 = f8;
    //     }
        
    //     // Getters
    //     public Integer getConnectedVertexId() { return connectedVertexId; }
    //     public String getDirection() { return direction; }
    //     public String getF1() { return f1; }
    //     public String getF2() { return f2; }
    //     public String getF3() { return f3; }
    //     public String getF4() { return f4; }
    //     public String getF5() { return f5; }
    //     public String getF6() { return f6; }
    //     public String getF7() { return f7; }
    //     public String getF8() { return f8; }
        
    //     @Override
    //     public String toString() {
    //         return String.format("EdgeInfo{connectedVertex='%s', direction='%s', f1='%s', f2='%s', f3='%s', f4='%s', f5='%s', f6='%s'}", 
    //                            connectedVertexId, direction, f1, f2, f3, f4, f5, f6);
    //     }
    // }
    
    // 示例用法
    public static void main(String[] args) {
        // Neo4j连接配置
        String uri = "bolt://localhost:7688";
        String username = "neo4j";
        String password = "password";
        int threadPoolSize = 10;
        
        Neo4jConcurrentGraphManager manager = new Neo4jConcurrentGraphManager(uri, username, password, threadPoolSize);
        
        try {
            // 清空数据库
            manager.clearDatabase();
            System.out.println("===  从文件加载图数据 ===");
            manager.loadGraphFromFile("/home/wsl-z/tugraph/web-Google.txt/web-NotreDame.txt");
            
            // read
            manager.read();

            // update
            manager.update();

            // p99
            
            
            
        } catch (Exception e) {
            System.err.println("exception e: " + e.getMessage());
        } finally {
            manager.close();
        }
    }
}