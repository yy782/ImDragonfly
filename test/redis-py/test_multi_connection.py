#!/usr/bin/env python3
"""
测试多连接场景下服务端稳定性的测试脚本

该测试创建多个并发连接，发送 SET/GET/DEL 命令，验证服务端在高并发下不会崩溃
"""

import threading
import time
import random
import sys
from datetime import datetime

try:
    import redis
except ImportError:
    print("请先安装 redis-py: pip install redis")
    sys.exit(1)

# 配置参数
HOST = 'localhost'
PORT = 6379
NUM_CONNECTIONS = 10
TEST_DURATION = 30  # 测试持续时间（秒）

def log(msg):
    """打印带时间戳的日志"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

class ConnectionWorker(threading.Thread):
    """连接工作线程，模拟客户端发送命令"""
    
    def __init__(self, worker_id, stop_event):
        super().__init__()
        self.worker_id = worker_id
        self.stop_event = stop_event
        self.client = None
        self.success_count = 0
        self.error_count = 0
    
    def run(self):
        """线程主循环"""
        log(f"Worker {self.worker_id} 启动")
        
        try:
            # 创建连接
            self.client = redis.Redis(host=HOST, port=PORT, decode_responses=True)
            
            while not self.stop_event.is_set():
                try:
                    self._execute_random_command()
                    self.success_count += 1
                except Exception as e:
                    self.error_count += 1
                    log(f"Worker {self.worker_id} 错误: {e}")
                    time.sleep(0.1)
            
        except Exception as e:
            log(f"Worker {self.worker_id} 连接失败: {e}")
        finally:
            if self.client:
                try:
                    self.client.close()
                except:
                    pass
            log(f"Worker {self.worker_id} 退出")
    
    def _execute_random_command(self):
        """执行随机 Redis 命令（仅支持 SET/GET/DEL）"""
        commands = [
            self._cmd_set,
            self._cmd_get,
            self._cmd_del,
        ]
        
        cmd = random.choice(commands)
        cmd()
    
    def _cmd_set(self):
        key = f"test:{self.worker_id}:{random.randint(0, 1000)}"
        value = f"value_{random.randint(0, 1000000)}"
        self.client.set(key, value)
    
    def _cmd_get(self):
        key = f"test:{self.worker_id}:{random.randint(0, 1000)}"
        self.client.get(key)
    
    def _cmd_del(self):
        key = f"test:{self.worker_id}:{random.randint(0, 1000)}"
        self.client.delete(key)

def main():
    """主测试函数"""
    log("=== 多连接稳定性测试 ===")
    log(f"配置: {NUM_CONNECTIONS} 个连接, 测试时长: {TEST_DURATION} 秒")
    log("支持命令: SET, GET, DEL")
    
    stop_event = threading.Event()
    workers = []
    
    # 创建工作线程
    for i in range(NUM_CONNECTIONS):
        worker = ConnectionWorker(i, stop_event)
        workers.append(worker)
    
    # 启动所有线程
    log(f"启动 {NUM_CONNECTIONS} 个工作线程...")
    for worker in workers:
        worker.start()
        time.sleep(0.05)
    
    # 等待测试结束
    log(f"测试运行中... ({TEST_DURATION} 秒)")
    time.sleep(TEST_DURATION)
    
    # 停止所有线程
    log("停止测试...")
    stop_event.set()
    
    # 等待所有线程完成
    log("等待线程退出...")
    for worker in workers:
        worker.join(timeout=5)
    
    # 汇总统计
    total_success = sum(w.success_count for w in workers)
    total_errors = sum(w.error_count for w in workers)
    
    log("\n=== 测试结果汇总 ===")
    log(f"总成功操作: {total_success}")
    log(f"总错误操作: {total_errors}")
    log(f"成功率: {total_success / max(total_success + total_errors, 1) * 100:.2f}%")
    log(f"服务端状态: {'正常' if total_errors < total_success * 0.01 else '需要关注'}")
    
    if total_errors > total_success * 0.01:
        log("警告: 错误率超过1%, 建议检查服务端状态")
        return 1
    else:
        log("测试通过: 服务端在多连接场景下运行正常")
        return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("测试被用户中断")
        sys.exit(0)
    except Exception as e:
        log(f"测试异常: {e}")
        sys.exit(1)
