#!/usr/bin/env python3
# python3 branchmark.py

import redis
import threading
import time
import argparse
import signal
from datetime import datetime

class ThroughputTester:
    def __init__(self, host='localhost', port=6379, num_workers=10, test_duration=10):
        self.host = host
        self.port = port
        self.num_workers = num_workers
        self.test_duration = test_duration
        
        # 统计信息
        self.stats_lock = threading.Lock()
        self.total_commands = 0
        self.stop_event = threading.Event()
        self.workers = []
        
        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        print(f"\n[Ctrl+C] 正在停止测试...")
        self.stop_event.set()
    
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    
    def worker(self, worker_id):
        """工作线程：持续发送SET和GET命令"""
        try:
            client = redis.Redis(
                host=self.host, 
                port=self.port,
                protocol=2,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            client.ping()
            self.log(f"Worker {worker_id}: 连接成功")
            
            key_prefix = f"bench:{worker_id}:{int(time.time())}"
            counter = 0
            local_count = 0
            
            while not self.stop_event.is_set():
                try:
                    # 交替执行SET和GET
                    key = f"{key_prefix}:{counter % 1000}"
                    value = f"value_{counter}"
                    
                    client.set(key, value)
                    client.get(key)
                    
                    counter += 1
                    local_count += 1
                    
                    # 清理旧数据
                    if counter % 100 == 0:
                        old_key = f"{key_prefix}:{(counter - 100) % 1000}"
                        client.delete(old_key)
                    
                except Exception as e:
                    self.log(f"Worker {worker_id}: 错误 - {e}")
                    # 重连
                    try:
                        client.close()
                    except:
                        pass
                    time.sleep(0.5)
                    client = redis.Redis(
                        host=self.host, 
                        port=self.port,
                        protocol=2,
                        decode_responses=True
                    )
                    client.ping()
            
            # 更新总统计
            with self.stats_lock:
                self.total_commands += local_count
            
            client.close()
            self.log(f"Worker {worker_id}: 退出，执行 {local_count} 条命令")
            
        except Exception as e:
            self.log(f"Worker {worker_id}: 连接失败 - {e}")
    
    def run_test(self):
        """运行测试"""
        self.log(f"\n{'='*60}")
        self.log(f"吞吐量测试")
        self.log(f"服务器: {self.host}:{self.port}")
        self.log(f"线程数: {self.num_workers}")
        self.log(f"测试时长: {self.test_duration} 秒")
        self.log(f"命令: SET + GET")
        self.log(f"{'='*60}\n")
        
        # 启动工作线程
        for i in range(self.num_workers):
            t = threading.Thread(target=self.worker, args=(i,))
            t.daemon = True
            self.workers.append(t)
            t.start()
            time.sleep(0.05)
        
        # 等待测试结束
        self.log(f"测试运行中...")
        start_time = time.time()
        
        # 每秒输出进度
        for i in range(self.test_duration):
            if self.stop_event.is_set():
                break
            time.sleep(1)
            with self.stats_lock:
                self.log(f"已处理: {self.total_commands} 条命令")
        
        # 停止所有线程
        self.stop_event.set()
        for t in self.workers:
            t.join(timeout=2)
        
        # 输出结果
        elapsed = time.time() - start_time
        qps = self.total_commands / elapsed if elapsed > 0 else 0
        
        self.log(f"\n{'='*60}")
        self.log(f"测试完成")
        self.log(f"运行时间: {elapsed:.2f} 秒")
        self.log(f"总命令数: {self.total_commands}")
        self.log(f"平均QPS: {qps:.2f} 请求/秒")
        self.log(f"{'='*60}\n")

def main():
    parser = argparse.ArgumentParser(description='仿Redis吞吐量测试')
    parser.add_argument('-n', '--workers', type=int, default=10,
                       help='工作线程数 (默认: 10)')
    parser.add_argument('-d', '--duration', type=int, default=10,
                       help='测试时长(秒) (默认: 10)')
    parser.add_argument('--host', default='localhost',
                       help='服务器地址 (默认: localhost)')
    parser.add_argument('--port', type=int, default=6379,
                       help='服务器端口 (默认: 6379)')
    
    args = parser.parse_args()
    
    tester = ThroughputTester(
        host=args.host,
        port=args.port,
        num_workers=args.workers,
        test_duration=args.duration
    )
    
    tester.run_test()

if __name__ == '__main__':
    main()