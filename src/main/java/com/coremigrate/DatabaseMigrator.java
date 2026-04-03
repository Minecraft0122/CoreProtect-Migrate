package com.coremigrate;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseMigrator {
    private static final String CONFIG_FILE = "migration.properties";
    private Connection sourceConnection;
    private Connection targetConnection;
    private Map<String, String> config = new HashMap<>();
    private Map<String, AtomicInteger> progressMap = new ConcurrentHashMap<>();
    private boolean backupBeforeMigrate = true;
    private int batchSize = 5000;
    private boolean autoCreateTables = true;

    public DatabaseMigrator() throws IOException, SQLException {
        loadConfig();
        initConnections();
    }

    private void loadConfig() throws IOException {
        System.out.println("Loading configuration from " + CONFIG_FILE);
        
        // 检查配置文件是否存在
        File config = new File(CONFIG_FILE);
        if (!config.exists()) {
            System.out.println("Configuration file not found! Creating example config...");
            createExampleConfig();
            System.out.println("Please edit migration.properties with your database details");
            System.out.println("Example config created at: " + CONFIG_FILE);
            System.exit(1);
        }

        // 加载配置
        try (BufferedReader reader = new BufferedReader(new FileReader(CONFIG_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("#")) continue;
                
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    config.put(parts[0].trim(), parts[1].trim());
                }
            }
        }

        // 设置默认值
        config.putIfAbsent("tables", "block,item,player,command");
        config.putIfAbsent("batch.size", String.valueOf(batchSize));
        config.putIfAbsent("auto.create.tables", String.valueOf(autoCreateTables));
        config.putIfAbsent("backup.before.migrate", String.valueOf(backupBeforeMigrate));
    }

    private void createExampleConfig() throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(CONFIG_FILE))) {
            writer.write("# Database Migration Configuration\n");
            writer.write("# SQLite Path (source)\n");
            writer.write("source.type=sqlite\n");
            writer.write("source.url=file:./coreprotect.db\n");
            writer.write("source.user=\n");
            writer.write("source.password=\n\n");
            
            writer.write("# MySQL Connection (target)\n");
            writer.write("target.type=mysql\n");
            writer.write("target.url=jdbc:mysql://localhost:3306/coreprotect\n");
            writer.write("target.user=root\n");
            writer.write("target.password=your_password_here\n");
            writer.write("target.prefix=\n");
        }
    }

    private void initConnections() throws SQLException {
        System.out.println("Initializing database connections...");
        
        // 源数据库
        String sourceType = config.get("source.type").toLowerCase();
        String sourceUrl = config.get("source.url");
        String sourceUser = config.get("source.user");
        String sourcePassword = config.get("source.password");
        
        // 目标数据库
        String targetType = config.get("target.type").toLowerCase();
        String targetUrl = config.get("target.url");
        String targetUser = config.get("target.user");
        String targetPassword = config.get("target.password");
        
        // 初始化源连接
        sourceConnection = createConnection(sourceUrl, sourceUser, sourcePassword, sourceType);
        System.out.println("Connected to source database: " + sourceType);
        
        // 初始化目标连接
        targetConnection = createConnection(targetUrl, targetUser, targetPassword, targetType);
        System.out.println("Connected to target database: " + targetType);
    }

    private Connection createConnection(String url, String user, String password, String type) throws SQLException {
        Connection connection;
        if (type.equals("sqlite")) {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection(url);
        } else {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, user, password);
        }
        return connection;
    }

    public void migrate() throws Exception {
        System.out.println("\nStarting database migration...");
        System.out.println("Source: " + config.get("source.type"));
        System.out.println("Target: " + config.get("target.type"));
        
        // 备份源数据库
        if (Boolean.parseBoolean(config.get("backup.before.migrate"))) {
            System.out.println("Backing up source database...");
            backupSourceDatabase();
        }
        
        // 获取要迁移的表
        List<String> tables = Arrays.asList(config.get("tables").split(","));
        System.out.println("Tables to migrate: " + tables);
        
        // 为每个表初始化进度跟踪
        for (String table : tables) {
            progressMap.put(table, new AtomicInteger(0));
        }
        
        // 创建迁移线程池
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(4, tables.size()));
        List<Future<Integer>> futures = new ArrayList<>();
        
        // 为每张表启动迁移
        for (String table : tables) {
            final String tableName = table.trim();
            futures.add(executor.submit(() -> migrateTable(tableName)));
        }
        
        // 显示进度
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    if (futures.stream().allMatch(Future::isDone)) {
                        break;
                    }
                    printProgress();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            printFinalProgress();
        }).start();
        
        // 等待所有迁移完成
        for (Future<Integer> future : futures) {
            future.get();
        }
        
        executor.shutdown();
        System.out.println("\nMigration completed successfully!");
    }

    private void backupSourceDatabase() throws IOException {
        String backupFile = "source_backup_" + System.currentTimeMillis() + ".db";
        String sourceUrl = config.get("source.url");
        
        // 处理SQLite路径
        String sourcePath = sourceUrl.replace("file:", "");
        if (sourcePath.startsWith("./")) {
            sourcePath = sourcePath.substring(2);
        }
        
        try (InputStream in = new FileInputStream(sourcePath);
             OutputStream out = new FileOutputStream(backupFile)) {
            
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        System.out.println("Source database backed up to: " + backupFile);
    }

    private int migrateTable(String table) throws SQLException {
        System.out.println("\nStarting migration for table: " + table);
        
        // 检查表是否存在
        if (!tableExists(table)) {
            System.out.println("Table " + table + " does not exist in source database. Skipping.");
            return 0;
        }
        
        // 获取表的总行数
        int totalCount = getRowCount(table);
        if (totalCount == 0) {
            System.out.println("Table " + table + " is empty. Skipping.");
            return 0;
        }
        
        System.out.println("Total records to migrate: " + totalCount);
        
        // 确保目标表存在
        if (autoCreateTables && !tableExistsInTarget(table)) {
            createTargetTable(table);
        }
        
        // 迁移表
        int processedCount = 0;
        int batchCount = 0;
        
        // 获取表结构信息
        try (Statement stmt = sourceConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " LIMIT 1")) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
            }
            
            // 准备插入语句
            String columnsList = String.join(", ", columnNames);
            String placeholders = String.join(", ", Collections.nCopies(columnNames.size(), "?"));
            String insertQuery = "INSERT INTO " + table + " (" + columnsList + ") VALUES (" + placeholders + ")";
            
            try (PreparedStatement insertStmt = targetConnection.prepareStatement(insertQuery)) {
                insertStmt.setQueryTimeout(3600);
                
                // 迁移数据
                long lastId = 0;
                int batchStart = 0;
                int batchEnd = batchSize;
                
                while (batchStart < totalCount) {
                    // 获取当前批次数据
                    String selectQuery = "SELECT * FROM " + table + " WHERE id > ? LIMIT ?";
                    try (PreparedStatement selectStmt = sourceConnection.prepareStatement(selectQuery)) {
                        selectStmt.setLong(1, lastId);
                        selectStmt.setInt(2, batchSize);
                        
                        try (ResultSet resultSet = selectStmt.executeQuery()) {
                            // 处理结果集
                            while (resultSet.next()) {
                                // 设置插入参数
                                for (int i = 1; i <= columnNames.size(); i++) {
                                    insertStmt.setObject(i, resultSet.getObject(i));
                                }
                                
                                insertStmt.addBatch();
                                processedCount++;
                                lastId = resultSet.getLong("id");
                                
                                // 执行批次
                                if (++batchCount >= batchSize) {
                                    insertStmt.executeBatch();
                                    insertStmt.clearBatch();
                                    batchCount = 0;
                                }
                            }
                        }
                    }
                    
                    // 更新进度
                    progressMap.get(table).set(processedCount);
                    
                    // 调整批处理大小
                    if (processedCount > 0 && batchStart % 10000 == 0) {
                        System.out.println("Migrated " + processedCount + " records for table " + table);
                    }
                    
                    batchStart = batchEnd;
                    batchEnd = Math.min(batchStart + batchSize, totalCount);
                }
                
                // 处理最后一批
                if (batchCount > 0) {
                    insertStmt.executeBatch();
                }
                
                System.out.println("Completed migration for table " + table + " (" + processedCount + " records)");
                return processedCount;
            }
        }
    }

    private boolean tableExists(String table) throws SQLException {
        try (ResultSet rs = sourceConnection.getMetaData().getTables(null, null, table, null)) {
            return rs.next();
        }
    }

    private boolean tableExistsInTarget(String table) throws SQLException {
        try (ResultSet rs = targetConnection.getMetaData().getTables(null, null, table, null)) {
            return rs.next();
        }
    }

    private void createTargetTable(String table) throws SQLException {
        System.out.println("Creating target table: " + table);
        
        try (Statement stmt = sourceConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " LIMIT 1")) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            StringBuilder createQuery = new StringBuilder("CREATE TABLE " + table + " (");
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String columnType = metaData.getColumnTypeName(i);
                
                // 处理MySQL类型映射
                if (targetConnection.getMetaData().getDatabaseProductName().toLowerCase().contains("mysql")) {
                    if (columnType.equalsIgnoreCase("INTEGER")) {
                        columnType = "INT";
                    } else if (columnType.equalsIgnoreCase("VARCHAR")) {
                        columnType = "VARCHAR(255)";
                    }
                }
                
                createQuery.append(columnName).append(" ").append(columnType);
                if (i < columnCount) createQuery.append(", ");
            }
            createQuery.append(")");
            
            try (Statement targetStmt = targetConnection.createStatement()) {
                targetStmt.execute(createQuery.toString());
            }
        }
    }

    private int getRowCount(String table) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + table;
        try (Statement stmt = sourceConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    private void printProgress() {
        System.out.print("\r");
        System.out.print("Migration progress: ");
        
        int totalProcessed = 0;
        int totalRecords = 0;
        
        for (Map.Entry<String, AtomicInteger> entry : progressMap.entrySet()) {
            totalProcessed += entry.getValue().get();
            totalRecords += getRowCount(entry.getKey());
        }
        
        double progress = totalRecords > 0 ? (double) totalProcessed / totalRecords : 0;
        int progressWidth = 50;
        int filledWidth = (int) Math.round(progress * progressWidth);
        
        System.out.print("[");
        for (int i = 0; i < filledWidth; i++) {
            System.out.print("=");
        }
        for (int i = filledWidth; i < progressWidth; i++) {
            System.out.print(" ");
        }
        System.out.print("] ");
        
        System.out.printf("%.1f%% (%d/%d)", progress * 100, totalProcessed, totalRecords);
    }

    private void printFinalProgress() {
        System.out.println("\n\nMigration completed!");
        System.out.println("Total records migrated: " + progressMap.values().stream().mapToInt(AtomicInteger::get).sum());
        
        System.out.println("\nDetailed progress:");
        for (Map.Entry<String, AtomicInteger> entry : progressMap.entrySet()) {
            int processed = entry.getValue().get();
            int total = getRowCount(entry.getKey());
            System.out.printf("  - %s: %d/%d records (%.1f%%)\n", entry.getKey(), processed, total, (total > 0) ? (processed * 100.0 / total) : 0);
        }
    }

    public static void main(String[] args) {
        System.out.println("CoreMigrate Database Migration Tool v1.0");
        System.out.println("=====================================");
        
        try {
            new DatabaseMigrator().migrate();
        } catch (Exception e) {
            System.err.println("Migration failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
