#!/usr/bin/env python3
import redis
import time
import sys
from datetime import datetime
# python3 performance_test.py
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

class PerformanceTest:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.client = None
    
    def connect(self):
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            self.client.ping()
            return True
        except redis.ConnectionError:
            return False
    
    def close(self):
        if self.client:
            self.client.close()
    
    def test_ping(self, iterations=10000):
        log(f"测试 PING x {iterations}")
        start = time.time()
        for i in range(iterations):
            self.client.ping()
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def test_set(self, iterations=100):
        log(f"测试 SET x {iterations}")
        start = time.time()
        for i in range(iterations):
            self.client.set(f"test:key:{i}", f"value:{i}")
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def test_get(self, iterations=100):
        log(f"测试 GET x {iterations}")
        start = time.time()
        for i in range(iterations):
            self.client.get(f"test:key:{i}")
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def test_set_get(self, iterations=100):
        log(f"测试 SET+GET x {iterations}")
        start = time.time()
        for i in range(iterations):
            self.client.set(f"test:sg:{i}", f"value:{i}")
            self.client.get(f"test:sg:{i}")
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def test_mset(self, iterations=1000):
        log(f"测试 MSET x {iterations}")
        start = time.time()
        for i in range(iterations):
            kv = {}
            for j in range(10):
                kv[f"test:mset:{i}:{j}"] = f"value:{i}:{j}"
            self.client.mset(kv)
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def test_mget(self, iterations=100):
        log(f"测试 MGET x {iterations}")
        start = time.time()
        for i in range(iterations):
            keys = [f"test:mset:{i}:{j}" for j in range(10)]
            self.client.mget(keys)
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def test_exists(self, iterations=100):
        log(f"测试 EXISTS x {iterations}")
        start = time.time()
        for i in range(iterations):
            self.client.exists(f"test:key:{i}")
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def test_expire(self, iterations=100):
        log(f"测试 EXPIRE x {iterations}")
        start = time.time()
        for i in range(iterations):
            key = f"test:exp:{i}"
            self.client.set(key, f"value:{i}")
            self.client.expire(key, 60)
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def test_ttl(self, iterations=100):
        log(f"测试 TTL x {iterations}")
        start = time.time()
        for i in range(iterations):
            self.client.ttl(f"test:exp:{i}")
        elapsed = time.time() - start
        qps = iterations / elapsed
        log(f"  耗时: {elapsed:.3f}s, QPS: {qps:.1f}")
        return qps
    
    def run_all_tests(self):
        results = {}
        log(f"\n=== 连接到 {self.host}:{self.port} ===")
        if not self.connect():
            log(f"  连接失败！")
            return None
        
        log(f"  连接成功")
        
        try:
            results['PING'] = self.test_ping()
            results['SET'] = self.test_set()
            results['GET'] = self.test_get()
            results['SET+GET'] = self.test_set_get()
            results['MSET'] = self.test_mset()
            results['MGET'] = self.test_mget()
            results['EXISTS'] = self.test_exists()
            results['EXPIRE'] = self.test_expire()
            results['TTL'] = self.test_ttl()
        finally:
            self.close()
        
        return results

def print_results(title, results):
    if results is None:
        print(f"{title}: 测试失败")
        return
    
    print(f"\n{title}")
    print("-" * 40)
    for cmd, qps in results.items():
        print(f"{cmd:10s} : {qps:>10.1f} QPS")

def print_comparison(imdragonfly_results, redis_results):
    print("\n" + "=" * 60)
    print("性能对比报告")
    print("=" * 60)
    print(f"{'命令':<10s} {'ImDragonfly':>12s} {'Redis':>12s} {'差距':>12s}")
    print("-" * 60)
    
    all_cmds = set(imdragonfly_results.keys()) | set(redis_results.keys())
    for cmd in sorted(all_cmds):
        idf_qps = imdragonfly_results.get(cmd, 0)
        redis_qps = redis_results.get(cmd, 0)
        
        if redis_qps > 0:
            ratio = (idf_qps / redis_qps) * 100
            ratio_str = f"{ratio:>6.1f}%"
        else:
            ratio_str = "N/A"
        
        print(f"{cmd:<10s} {idf_qps:>12.1f} {redis_qps:>12.1f} {ratio_str:>12s}")
    
    print("=" * 60)

def main():
    print("=" * 60)
    print("ImDragonfly vs Redis 性能对比测试")
    print("=" * 60)
    
    idf_test = PerformanceTest(host='localhost', port=6379)
    redis_test = PerformanceTest(host='localhost', port=6380)
    
    print("\n" + "=" * 60)
    print("测试 ImDragonfly (localhost:6379)")
    print("=" * 60)
    idf_results = idf_test.run_all_tests()
    
    print("\n" + "=" * 60)
    print("测试 Redis (localhost:6380)")
    print("=" * 60)
    redis_results = redis_test.run_all_tests()
    
    print_comparison(idf_results, redis_results)
    
    if idf_results is None:
        print("\n警告: ImDragonfly 测试失败，请确保服务运行在端口 6379")
    if redis_results is None:
        print("\n警告: Redis 测试失败，请确保服务运行在端口 6380")

if __name__ == "__main__":
    main()