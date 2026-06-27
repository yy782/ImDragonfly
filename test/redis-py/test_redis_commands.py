# python3 test_redis_commands.py

import redis
import time
import sys
from typing import Any

class RedisCommandTester:
    def __init__(self):
        self.r = None
        self.passed = 0
        self.failed = 0
        self.test_keys = []
    
    def connect(self) -> bool:
        try:
            self.r = redis.Redis(
                host='localhost',
                port=6379,
                db=0,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            self.r.ping()
            print("✓ 成功连接到 Redis")
            return True
        except redis.ConnectionError:
            print("✗ 连接失败: 请确保 Redis 正在运行")
            return False
    
    def cleanup(self):
        for key in self.test_keys:
            try:
                self.r.delete(key)
            except:
                pass
    
    def add_test_key(self, key: str):
        if key not in self.test_keys:
            self.test_keys.append(key)
    
    def test(self, name: str, expected, actual) -> bool:
        if expected == actual:
            print(f"  ✓ {name}: {actual}")
            self.passed += 1
            return True
        else:
            print(f"  ✗ {name}: 期望 {expected}, 实际 {actual}")
            self.failed += 1
            return False
    
    def test_section(self, title: str):
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
    
    def run_all_tests(self):
        if not self.connect():
            sys.exit(1)
        
        try:
            self.test_section("基本命令测试 (SET, GET, MSET, MGET, DEL, EXISTS)")
            self.test_basic_commands()
            
            self.test_section("过期命令测试 (EXPIRE, PERSIST, TTL, EXPIRETIME)")
            self.test_expire_commands()
            
            self.test_section("列表命令测试 (LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LSET, LRANGE, LREM, LINSERT)")
            self.test_list_commands()
            
            self.test_section("哈希命令测试 (HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL)")
            self.test_hash_commands()
            
            self.test_section("集合命令测试 (SADD, SREM, SMEMBERS, SCARD, SISMEMBER)")
            self.test_set_commands()
            
            self.test_section("有序集合命令测试 (ZADD, ZCARD, ZSCORE, ZREM, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE)")
            self.test_zset_commands()
            
            self.test_section("事务命令测试 (MULTI, EXEC, DISCARD, WATCH, UNWATCH)")
            self.test_transaction_commands()
            
            self.test_section("其他命令测试 (PING, ECHO)")
            self.test_misc_commands()
            
            print(f"\n{'='*60}")
            print(f"  测试结果: {self.passed} 通过, {self.failed} 失败")
            print(f"{'='*60}")
            
            if self.failed > 0:
                sys.exit(1)
        
        finally:
            self.cleanup()
    
    def test_basic_commands(self):
        key = "test:basic:key"
        self.add_test_key(key)
        
        self.test("SET", True, self.r.set(key, "hello"))
        self.test("GET", "hello", self.r.get(key))
        
        key2 = "test:basic:key2"
        self.add_test_key(key2)
        
        self.test("MSET", True, self.r.mset({key: "value1", key2: "value2"}))
        self.test("MGET", ["value1", "value2"], self.r.mget([key, key2]))
        
        self.test("EXISTS", 2, self.r.exists(key, key2))
        self.test("DEL", 2, self.r.delete(key, key2))
        self.test("EXISTS after DEL", 0, self.r.exists(key, key2))
        self.test("GET after DEL", None, self.r.get(key))
    
    def test_expire_commands(self):
        key = "test:expire:key"
        self.add_test_key(key)
        
        self.r.set(key, "expire_value")
        
        self.test("EXPIRE", True, self.r.expire(key, 10))
        ttl = self.r.ttl(key)
        self.test("TTL > 0", ttl > 0, True)
        
        self.test("PERSIST", True, self.r.persist(key))
        self.test("TTL after PERSIST", -1, self.r.ttl(key))
        
        self.r.expire(key, 5)
        expiretime = self.r.expiretime(key)
        self.test("EXPIRETIME > 0", expiretime > 0, True)
    
    def test_list_commands(self):
        key = "test:list:key"
        self.add_test_key(key)
        
        self.test("LPUSH", 3, self.r.lpush(key, "a", "b", "c"))
        self.test("LLEN", 3, self.r.llen(key))
        self.test("LRANGE", ["c", "b", "a"], self.r.lrange(key, 0, -1))
        self.test("LINDEX", "c", self.r.lindex(key, 0))
        self.test("LINDEX -1", "a", self.r.lindex(key, -1))
        
        self.test("RPUSH", 4, self.r.rpush(key, "d"))
        self.test("LRANGE after RPUSH", ["c", "b", "a", "d"], self.r.lrange(key, 0, -1))
        
        self.test("LSET", True, self.r.lset(key, 1, "x"))
        self.test("LRANGE after LSET", ["c", "x", "a", "d"], self.r.lrange(key, 0, -1))
        
        self.test("LPOP", "c", self.r.lpop(key))
        self.test("RPOP", "d", self.r.rpop(key))
        self.test("LLEN after POP", 2, self.r.llen(key))
        
        self.r.rpush(key, "x")
        self.test("LREM", 2, self.r.lrem(key, 0, "x"))
        self.test("LLEN after LREM", 1, self.r.llen(key))
        
        self.test("LINSERT AFTER", 2, self.r.linsert(key, "AFTER", "a", "new"))
        self.test("LRANGE after LINSERT", ["a", "new"], self.r.lrange(key, 0, -1))
    
    def test_hash_commands(self):
        key = "test:hash:key"
        self.add_test_key(key)
        
        self.test("HSET", 1, self.r.hset(key, "field1", "value1"))
        self.test("HSET another", 1, self.r.hset(key, "field2", "value2"))
        
        self.test("HGET", "value1", self.r.hget(key, "field1"))
        self.test("HGET non-existent", None, self.r.hget(key, "field3"))
        
        self.test("HEXISTS", True, self.r.hexists(key, "field1"))
        self.test("HEXISTS non-existent", False, self.r.hexists(key, "field3"))
        
        self.test("HLEN", 2, self.r.hlen(key))
        
        hgetall_result = self.r.hgetall(key)
        self.test("HGETALL keys", {"field1", "field2"}, set(hgetall_result.keys()))
        
        self.test("HDEL", 1, self.r.hdel(key, "field1"))
        self.test("HLEN after HDEL", 1, self.r.hlen(key))
    
    def test_set_commands(self):
        key = "test:set:key"
        self.add_test_key(key)
        
        self.test("SADD", 3, self.r.sadd(key, "a", "b", "c"))
        self.test("SADD duplicate", 0, self.r.sadd(key, "a"))
        
        self.test("SCARD", 3, self.r.scard(key))
        
        self.test("SISMEMBER", True, self.r.sismember(key, "a"))
        self.test("SISMEMBER non-existent", False, self.r.sismember(key, "d"))
        
        smembers_result = self.r.smembers(key)
        self.test("SMEMBERS", {"a", "b", "c"}, smembers_result)
        
        self.test("SREM", 2, self.r.srem(key, "a", "b"))
        self.test("SCARD after SREM", 1, self.r.scard(key))
    
    def test_zset_commands(self):
        key = "test:zset:key"
        self.add_test_key(key)
        
        self.test("ZADD", 3, self.r.execute_command('ZADD', key, 1, "a", 2, "b", 3, "c"))
        
        self.test("ZCARD", 3, self.r.execute_command('ZCARD', key))
        
        self.test("ZSCORE", "1", self.r.execute_command('ZSCORE', key, "a"))
        
        self.test("ZRANK", 0, self.r.execute_command('ZRANK', key, "a"))
        self.test("ZRANK last", 2, self.r.execute_command('ZRANK', key, "c"))
        
        self.test("ZREVRANK", 2, self.r.execute_command('ZREVRANK', key, "a"))
        self.test("ZREVRANK first", 0, self.r.execute_command('ZREVRANK', key, "c"))
        
        self.test("ZRANGE", ["a", "b"], self.r.execute_command('ZRANGE', key, 0, 1))
        self.test("ZREVRANGE", ["c", "b"], self.r.execute_command('ZREVRANGE', key, 0, 1))
        
        self.test("ZREM", 1, self.r.execute_command('ZREM', key, "b"))
        self.test("ZCARD after ZREM", 2, self.r.execute_command('ZCARD', key))
    
    def test_transaction_commands(self):
        key1 = "test:tx:key1"
        key2 = "test:tx:key2"
        self.add_test_key(key1)
        self.add_test_key(key2)
        
        self.r.set(key1, "100")
        self.r.set(key2, "200")
        
        pipeline = self.r.pipeline()
        pipeline.multi()
        pipeline.set(key1, "150")
        pipeline.set(key2, "250")
        results = pipeline.execute()
        self.test("MULTI+EXEC", [True, True], results)
        
        self.test("GET after transaction", "150", self.r.get(key1))
        self.test("GET after transaction", "250", self.r.get(key2))
        
        pipeline2 = self.r.pipeline()
        pipeline2.multi()
        pipeline2.set(key1, "should_not_set")
        pipeline2.discard()
        self.test("GET after DISCARD", "150", self.r.get(key1))
        
        self.r.watch(key1)
        pipeline3 = self.r.pipeline()
        pipeline3.multi()
        pipeline3.set(key1, "watched_value")
        results3 = pipeline3.execute()
        self.test("WATCH+EXEC", [True], results3)
        self.test("GET after WATCH+EXEC", "watched_value", self.r.get(key1))
        
        self.r.unwatch()
    
    def test_misc_commands(self):
        self.test("PING", True, self.r.ping())
        self.test("ECHO", "hello", self.r.echo("hello"))

def main():
    tester = RedisCommandTester()
    tester.run_all_tests()

if __name__ == "__main__":
    main()