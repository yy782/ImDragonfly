#!/usr/bin/env python3
"""
测试 Redis 连接池和多连接场景下服务端稳定性

该测试模拟真实场景中的连接池使用模式，验证服务端在大量连接复用情况下的稳定性
仅支持 SET/GET/DEL 命令
"""

import threading
import time
import random
import sys
from datetime import datetime

try:
    import redis
    from redis.connection import ConnectionPool
except ImportError:
    print("请先安装 redis-py: pip install redis")
    sys.exit(1)

# 配置参数
HOST = 'localhost'
PORT = 6379
POOL_SIZE = 20
NUM_WORKERS = 50
TEST_DURATION = 60  # 测试持续时间（秒）

def log(msg):
    """打印带时间戳的日志"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

class PoolWorker(threading.Thread):
    """使用连接池的工作线程"""
    
    def __init__(self, worker_id, pool, stop_event):
        super().__init__()
        self.worker_id = worker_id
        self.pool = pool
        self.stop_event = stop_event
        self.success_count = 0
        self.error_count = 0
        self.connection_reuse_count = 0
    
    def run(self):
        """线程主循环"""
        log(f"PoolWorker {self.worker_id} 启动")
        
        try:
            while not self.stop_event.is_set():
                try:
                    # 从连接池获取连接
                    client = redis.Redis(connection_pool=self.pool)
                    
                    # 执行多个命令复用同一个连接
                    for _ in range(random.randint(1, 5)):
                        self._execute_random_command(client)
                        self.success_count += 1
                        self.connection_reuse_count += 1
                    
                    # 显式释放连接回池
                    client.connection_pool.release(client.connection)
                    
                except Exception as e:
                    self.error_count += 1
                    log(f"PoolWorker {self.worker_id} 错误: {e}")
                    time.sleep(0.1)
            
        except Exception as e:
            log(f"PoolWorker {self.worker_id} 异常: {e}")
        
        log(f"PoolWorker {self.worker_id} 退出")
    
    def _execute_random_command(self, client):
        """执行随机 Redis 命令（仅支持 SET/GET/DEL）"""
        commands = [
            lambda: client.set(f"pool_test:{self.worker_id}:{random.randint(0, 10000)}", 
                              f"value_{random.random()}"),
            lambda: client.get(f"pool_test:{self.worker_id}:{random.randint(0, 10000)}"),
            lambda: client.delete(f"pool_test:{self.worker_id}:{random.randint(0, 10000)}"),
        ]
        
        cmd = random.choice(commands)
        cmd()

def main():
    """主测试函数"""
    log("=== 连接池稳定性测试 ===")
    log(f"配置: 连接池大小={POOL_SIZE}, 工作线程数={NUM_WORKERS}, 测试时长={TEST_DURATION}秒")
    log("支持命令: SET, GET, DEL")
    
    # 创建连接池
    log("创建连接池...")
    try:
        pool = ConnectionPool(host=HOST, port=PORT, max_connections=POOL_SIZE, decode_responses=True)
    except Exception as e:
        log(f"连接池创建失败: {e}")
        return 1
    
    stop_event = threading.Event()
    workers = []
    
    # 创建工作线程
    log(f"创建 {NUM_WORKERS} 个工作线程...")
    for i in range(NUM_WORKERS):
        worker = PoolWorker(i, pool, stop_event)
        workers.append(worker)
    
    # 启动所有线程
    log(f"启动工作线程...")
    start_time = time.time()
    for worker in workers:
        worker.start()
        time.sleep(0.02)
    
    # 定期输出统计
    last_time = start_time
    while not stop_event.is_set():
        elapsed = time.time() - start_time
        if elapsed >= TEST_DURATION:
            break
        
        if time.time() - last_time >= 10:
            total_success = sum(w.success_count for w in workers)
            total_errors = sum(w.error_count for w in workers)
            log(f"运行中 [{int(elapsed)}s] - 成功: {total_success}, 错误: {total_errors}")
            last_time = time.time()
        
        time.sleep(0.1)
    
    # 停止所有线程
    log("停止测试...")
    stop_event.set()
    
    # 等待所有线程完成
    log("等待线程退出...")
    for worker in workers:
        worker.join(timeout=10)
    
    # 汇总统计
    total_success = sum(w.success_count for w in workers)
    total_errors = sum(w.error_count for w in workers)
    total_reuse = sum(w.connection_reuse_count for w in workers)
    elapsed_time = time.time() - start_time
    
    log("\n=== 测试结果汇总 ===")
    log(f"总成功操作: {total_success}")
    log(f"总错误操作: {total_errors}")
    log(f"连接复用次数: {total_reuse}")
    log(f"测试时长: {elapsed_time:.2f} 秒")
    log(f"操作速率: {total_success / elapsed_time:.2f} ops/sec")
    log(f"成功率: {total_success / max(total_success + total_errors, 1) * 100:.2f}%")
    
    # 关闭连接池
    try:
        pool.disconnect()
        log("连接池已关闭")
    except Exception as e:
        log(f"关闭连接池时出错: {e}")
    
    if total_errors > total_success * 0.01:
        log("警告: 错误率超过1%, 建议检查服务端状态")
        return 1
    else:
        log("测试通过: 服务端在连接池场景下运行正常")
        return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("测试被用户中断")
        sys.exit(0)
    except Exception as e:
        log(f"测试异常: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
