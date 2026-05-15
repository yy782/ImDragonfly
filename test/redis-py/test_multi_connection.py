#!/usr/bin/env python3
import redis
import threading
import time
from datetime import datetime

running_time = 10

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def worker_with_continuous_commands(worker_id, stop_event, cmd_interval):
    """工作线程：持续发送命令"""
    try:
        client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        client.ping()
        log(f"Worker {worker_id}: 连接成功")
        
        count = 0
        while not stop_event.is_set():
            try:
                key = f"test:{worker_id}:{count % 100}"
                value = f"value_{count}"
                client.set(key, value)
                client.get(key)
                client.delete(key)
                count += 1
                
                if cmd_interval > 0:
                    time.sleep(cmd_interval)
                    
            except Exception as e:
                log(f"Worker {worker_id}: 错误 (第{count}次) - {e}")
                # 尝试重连
                try:
                    client.close()
                except:
                    pass
                client = redis.Redis(host='localhost', port=6379, decode_responses=True)
                client.ping()
                log(f"Worker {worker_id}: 重连成功")
                time.sleep(0.5)
        log(f"Worker {worker_id}: 准备Close")
        client.close()
        log(f"Worker {worker_id}: 退出，共执行 {count} 次命令")
        
    except Exception as e:
        log(f"Worker {worker_id}: 连接失败 - {e}")

def test_concurrent_with_interval(interval_ms):
    """测试不同命令间隔下的并发表现"""
    log(f"\n=== 测试命令间隔: {interval_ms}ms ===")
    
    stop_event = threading.Event()
    workers = []
    
    for i in range(5):  # 5个并发线程
        t = threading.Thread(target=worker_with_continuous_commands, 
                            args=(i, stop_event, interval_ms/1000.0))
        workers.append(t)
        t.start()
        time.sleep(0.2)  # 错开启动
    

    time.sleep(running_time)
    stop_event.set()
    
    for t in workers:
        t.join(timeout=5)
    
    log(f"测试完成 (间隔: {interval_ms}ms)\n")

if __name__ == "__main__":

    test_concurrent_with_interval(10)     # 10ms间隔
