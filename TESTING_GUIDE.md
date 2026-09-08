# StarRocks Connector Docker 重定向问题修复 - 测试指南

## 🧪 测试方案概述

本测试方案确保我们的容器感知重定向策略修复方案的可靠性和性能。

## 1. 单元测试

### 1.1 运行基础单元测试

```bash
# 进入stream-load-sdk目录
cd starrocks-stream-load-sdk

# 运行重定向策略测试
mvn test -Dtest=ContainerAwareRedirectStrategyTest

# 运行所有单元测试
mvn test
```

### 1.2 测试覆盖的场景

- ✅ 正常重定向（外部主机）保持不变
- ✅ localhost/127.0.0.1 重定向自动修复
- ✅ 多URL映射功能
- ✅ HTTP/HTTPS 支持
- ✅ 端口映射逻辑

## 2. 集成测试

### 2.1 创建测试环境

创建测试用的 Docker Compose 文件：

```yaml
# docker-compose-test.yml
version: '3.8'
services:
  starrocks-test:
    image: starrocks/allin1-ubuntu:3.5.0
    container_name: starrocks-test
    ports:
      - "18030:8030"  # FE HTTP端口
      - "19030:9030"  # FE MySQL端口  
      - "18040:8040"  # BE HTTP端口
    environment:
      - TZ=Asia/Shanghai
    networks:
      test-network:
        ipv4_address: 172.26.0.10

  flink-test:
    image: custom-flink:1.20.1-paimon
    container_name: flink-test
    depends_on:
      - starrocks-test
    volumes:
      - ./target:/opt/flink/usrlib
      - ./test-data:/data
    networks:
      test-network:
        ipv4_address: 172.26.0.20

networks:
  test-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.26.0.0/16
```

### 2.2 集成测试脚本

```bash
#!/bin/bash
# integration-test.sh

echo "=== StarRocks连接器重定向修复集成测试 ==="

# 1. 启动测试环境
echo "1. 启动测试环境..."
docker-compose -f docker-compose-test.yml up -d

# 等待服务启动
echo "2. 等待服务启动..."
sleep 30

# 3. 验证网络连通性
echo "3. 验证网络连通性..."
docker exec flink-test curl -I http://starrocks-test:8030 || {
    echo "❌ 网络连通性测试失败"
    exit 1
}

# 4. 测试场景1：使用修复前的配置（单一load-url）
echo "4. 测试修复后的配置..."
docker exec flink-test /opt/flink/bin/sql-client.sh -f /data/test-fix.sql || {
    echo "❌ 修复后配置测试失败"
    exit 1
}

# 5. 测试场景2：验证向后兼容性（多load-url）
echo "5. 测试向后兼容性..."
docker exec flink-test /opt/flink/bin/sql-client.sh -f /data/test-backward.sql || {
    echo "❌ 向后兼容性测试失败"
    exit 1
}

echo "✅ 集成测试通过！"

# 清理
docker-compose -f docker-compose-test.yml down
```

### 2.3 SQL测试文件

创建测试SQL文件：

```sql
-- test-data/test-fix.sql
-- 测试修复后的单一load-url配置

CREATE TABLE test_redirect_fix (
    id INT,
    name STRING,
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks-test:9030',
    'load-url' = 'starrocks-test:8030',  -- 修复后：单一URL即可工作
    'database-name' = 'test_db',
    'table-name' = 'test_table',
    'username' = 'root',
    'password' = '',
    'sink.semantic' = 'at-least-once',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true'
);

-- 插入测试数据
INSERT INTO test_redirect_fix VALUES 
(1, 'test_fix', CURRENT_TIMESTAMP),
(2, 'redirect_works', CURRENT_TIMESTAMP);
```

```sql
-- test-data/test-backward.sql  
-- 测试向后兼容性（多URL配置）

CREATE TABLE test_backward_compat (
    id INT,
    name STRING,
    create_time TIMESTAMP(3)
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks-test:9030',
    'load-url' = 'starrocks-test:8030;starrocks-test:8040',  -- 兼容性：多URL配置
    'database-name' = 'test_db',
    'table-name' = 'test_table2',
    'username' = 'root',
    'password' = '',
    'sink.semantic' = 'at-least-once',
    'sink.properties.format' = 'json',
    'sink.properties.strip_outer_array' = 'true'
);

-- 插入测试数据
INSERT INTO test_backward_compat VALUES 
(3, 'backward_compat', CURRENT_TIMESTAMP),
(4, 'still_works', CURRENT_TIMESTAMP);
```

## 3. 性能测试

### 3.1 基准测试

```bash
#!/bin/bash
# benchmark-test.sh

echo "=== 性能基准测试 ==="

# 测试修复前后的性能差异
echo "1. 测试修复前性能（多URL配置）..."
time docker exec flink-test /opt/flink/bin/sql-client.sh -f /data/benchmark-before.sql

echo "2. 测试修复后性能（单URL配置）..."
time docker exec flink-test /opt/flink/bin/sql-client.sh -f /data/benchmark-after.sql

echo "3. 分析结果..."
# 比较两次执行时间，确保性能影响 < 5%
```

### 3.2 压力测试

```sql
-- benchmark-stress.sql
-- 大量并发插入测试

-- 创建测试表
CREATE TABLE stress_test (
    id BIGINT,
    data STRING,
    timestamp_col TIMESTAMP(3)
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://starrocks-test:9030',
    'load-url' = 'starrocks-test:8030',
    'database-name' = 'test_db',
    'table-name' = 'stress_test',
    'username' = 'root',
    'password' = '',
    'sink.buffer-flush.max-rows' = '10000',
    'sink.buffer-flush.interval-ms' = '1000',
    'sink.semantic' = 'at-least-once'
);

-- 批量插入测试数据（模拟高并发场景）
INSERT INTO stress_test SELECT 
    ROW_NUMBER() OVER() as id,
    CONCAT('test_data_', CAST(ROW_NUMBER() OVER() AS STRING)) as data,
    CURRENT_TIMESTAMP as timestamp_col
FROM TABLE(GENERATE_SERIES(1, 100000));
```

## 4. 回归测试

### 4.1 现有功能验证

```bash
# 运行现有的所有测试用例
mvn test

# 运行集成测试
mvn integration-test

# 验证现有连接器功能
./run-existing-tests.sh
```

### 4.2 兼容性矩阵

| 配置方式 | 修复前状态 | 修复后状态 | 测试结果 |
|----------|------------|------------|----------|
| 单一URL (8030) | ❌ 失败 | ✅ 成功 | 待测试 |
| 多URL (8030;8040) | ✅ 成功 | ✅ 成功 | 待测试 |
| 直连BE (8040) | ⚠️ 部分成功 | ⚠️ 部分成功 | 待测试 |

## 5. 自动化测试

### 5.1 CI/CD 集成

```yaml
# .github/workflows/test-redirect-fix.yml
name: Test Redirect Fix

on:
  push:
    paths:
      - 'starrocks-stream-load-sdk/**'
  pull_request:
    paths:
      - 'starrocks-stream-load-sdk/**'

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up JDK 8
      uses: actions/setup-java@v2
      with:
        java-version: '8'
        distribution: 'adopt'
    
    - name: Run Unit Tests
      run: |
        cd starrocks-stream-load-sdk
        mvn test -Dtest=ContainerAwareRedirectStrategyTest
    
    - name: Run Integration Tests
      run: |
        docker-compose -f docker-compose-test.yml up -d
        sleep 30
        ./integration-test.sh
        docker-compose -f docker-compose-test.yml down
    
    - name: Performance Test
      run: ./benchmark-test.sh
```

## 6. 测试执行清单

### 6.1 预提交检查

- [ ] 单元测试通过
- [ ] Checkstyle检查通过
- [ ] 集成测试通过
- [ ] 性能测试通过
- [ ] 回归测试通过

### 6.2 发布前验证

- [ ] 多环境测试（Docker、K8s等）
- [ ] 多版本兼容性验证
- [ ] 文档更新验证
- [ ] 用户接受测试

## 7. 问题排查

### 7.1 常见问题

**Q: 单元测试编译失败？**
A: 检查是否有未使用的import，运行checkstyle检查

**Q: 集成测试连接失败？**
A: 
```bash
# 检查网络连通性
docker exec flink-test ping starrocks-test

# 检查端口开放
docker exec flink-test telnet starrocks-test 8030
```

**Q: 性能测试结果异常？**
A: 多次运行取平均值，排除环境因素影响

### 7.2 调试方法

```bash
# 开启详细日志
export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG"

# 查看重定向日志
docker logs flink-test | grep -i redirect

# 抓包分析
docker exec flink-test tcpdump -i eth0 -w /tmp/traffic.pcap host starrocks-test
```

## 8. 测试报告模板

```markdown
## 测试报告

### 测试环境
- OS: Ubuntu 20.04
- Docker: 20.10.x
- Java: 8
- Maven: 3.8.x

### 测试结果
- 单元测试: ✅ 通过 (5/5)
- 集成测试: ✅ 通过 (场景1: 成功, 场景2: 成功)
- 性能测试: ✅ 通过 (性能影响 < 2%)
- 回归测试: ✅ 通过 (现有功能正常)

### 发现问题
- 无

### 建议
- 可以合并到主分支
```

---

按照此测试指南执行，可以确保修复方案的质量和可靠性。建议按顺序执行所有测试步骤。 