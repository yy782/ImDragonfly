# python3 redis1.py
# cd test/redis-py
import redis
import sys
from typing import List, Tuple, Optional, Any
# cd programs/ImDragonfly
class RedisClientWithTransaction:
    def __init__(self):
        self.r = None
        self.in_transaction = False
        self.current_pipeline = None
        self.watched_keys = set()
        
    def connect(self) -> bool:
        """连接 Redis"""
        try:
            self.r = redis.Redis(
                host='localhost',
                port=6379,
                db=0,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            self.r.ping()
            print("✓ 成功连接到 Redis\n")
            return True
        except redis.ConnectionError:
            print("✗ 连接失败: 无法连接到 Redis 服务器")
            print("  请确保 Redis 正在运行: redis-server")
            return False
    
    def reset_transaction_state(self):
        """重置事务状态"""
        self.in_transaction = False
        self.current_pipeline = None
    
    def parse_command(self, cmd: str) -> Tuple[str, List[str]]:
        """解析命令，返回（命令名，参数列表）"""
        parts = cmd.strip().split()
        if not parts:
            return "", []
        return parts[0].upper(), parts[1:]
    
    def format_result(self, result: Any) -> str:
        """格式化输出结果"""
        if result is None:
            return "(nil)"
        elif isinstance(result, bool):
            return "OK" if result else "FAIL"
        elif isinstance(result, int):
            return f"(integer) {result}"
        elif isinstance(result, str):
            return result
        elif isinstance(result, list):
            if not result:
                return "(empty array)"
            return "\n".join([f"  {i+1}) {v if v is not None else '(nil)'}" for i, v in enumerate(result)])
        elif isinstance(result, dict):
            return str(result)
        else:
            return str(result)
    
    def print_result(self, result: Any, prefix: str = ""):
        """打印结果"""
        if prefix:
            print(f"{prefix}{self.format_result(result)}")
        else:
            print(self.format_result(result))
    
    def handle_watch(self, keys: List[str]):
        """处理 WATCH 命令"""
        try:
            for key in keys:
                result = self.r.execute_command('WATCH', key)
                if self.format_result(result) != 'OK':
                    raise Exception(f"WATCH命令失败: 期望 'OK'，实际得到 '{result}'")
            self.watched_keys.update(keys)
            self.print_result(result)
        except Exception as e:
            print(f"(error) {e}")
    
    def handle_unwatch(self):
        """处理 UNWATCH 命令"""
        try:
            result = self.r.execute_command('UNWATCH')
            self.watched_keys.clear()
            self.print_result(result)
        except Exception as e:
            print(f"(error) {e}")
    
    def handle_multi(self):
        """处理 MULTI 命令 - 开启事务"""
        if self.in_transaction:
            print("(error) ERR MULTI 不能嵌套调用")
            return
        
        try:
            # 直接发送 MULTI 命令，获取服务器响应
            result = self.r.execute_command('MULTI')
            self.in_transaction = True
            self.print_result(result)
        except Exception as e:
            print(f"(error) {e}")
    
    def handle_discard(self):
        """处理 DISCARD 命令 - 取消事务"""
        if not self.in_transaction:
            print("(error) ERR 不在事务模式中")
            return
        
        try:
            # 直接发送 DISCARD 命令
            result = self.r.execute_command('DISCARD')
            self.print_result(result)
        except Exception as e:
            print(f"(error) {e}")
        finally:
            self.reset_transaction_state()
            self.watched_keys.clear()
    
    def handle_exec(self):
        """处理 EXEC 命令 - 执行事务"""
        if not self.in_transaction:
            print("(error) ERR 不在事务模式中")
            return
        
        try:
            # 直接发送 EXEC 命令，获取批量结果
            results = self.r.execute_command('EXEC')
            
            if results is None:
                print("(nil)")
            else:
                for i, result in enumerate(results, 1):
                    if isinstance(result, Exception):
                        print(f"{i}. (error) {str(result)}")
                    else:
                        print(f"{i}. {self.format_result(result)}")
        except redis.exceptions.ResponseError as e:
            if "EXECABORT" in str(e):
                print(f"(error) {e}")
            else:
                print(f"(error) {e}")
        except Exception as e:
            print(f"(error) {e}")
        finally:
            self.reset_transaction_state()
            self.watched_keys.clear()
    
    def handle_transaction_command(self, cmd_name: str, args: List[str]) -> bool:
        """
        处理事务模式中的命令
        每条命令立即发送给服务器，打印服务器返回的真实响应
        """
        if cmd_name in ['MULTI', 'WATCH', 'UNWATCH']:
            print(f"(error) ERR {cmd_name} 不能在事务模式中使用")
            return False
        elif cmd_name == 'EXEC':
            self.handle_exec()
            return False
        elif cmd_name == 'DISCARD':
            self.handle_discard()
            return False
        
        try:
            # 构建完整的 Redis 命令字符串
            # 注意：需要对包含空格的参数加引号
            cmd_parts = [cmd_name] + args
            cmd_str = ' '.join(cmd_parts)
            
            # 直接发送命令到 Redis 服务器
            # 在事务模式下，服务器会返回 QUEUED
            result = self.r.execute_command(cmd_str)
            
            # 输出服务器返回的真实结果（应该是 "QUEUED"）
            self.print_result(result)
            
            
        except Exception as e:
            print(f"(error) {e}")
            # 如果命令入队失败，事务应该被放弃
            self.handle_discard()
        
        return False
    
    def handle_normal_command(self, cmd_name: str, args: List[str]) -> bool:
        """处理普通命令"""
        try:
            if cmd_name == 'SET' and len(args) == 2:
                key, value = args
                result = self.r.set(key, value)
                self.print_result(result)
                
            elif cmd_name == 'GET' and len(args) == 1:
                key = args[0]
                result = self.r.get(key)
                self.print_result(result)
                    
            elif cmd_name == 'MSET':
                if len(args) < 2 or len(args) % 2 != 0:
                    print("错误: MSET 需要 key-value 对，格式: MSET key1 value1 key2 value2 ...")
                    return False
                
                kv_pairs = {}
                for i in range(0, len(args), 2):
                    kv_pairs[args[i]] = args[i+1]
                
                result = self.r.mset(kv_pairs)
                self.print_result(result)
                
            elif cmd_name == 'MGET':
                if len(args) < 1:
                    print("错误: MGET 需要至少一个 key")
                    return False
                
                results = self.r.mget(args)
                for i, (key, value) in enumerate(zip(args, results), 1):
                    if value is None:
                        print(f"{i}) {key}: (nil)")
                    else:
                        print(f"{i}) {key}: {value}")
                        
            elif cmd_name == 'DEL':
                if len(args) < 1:
                    print("错误: DEL 需要至少一个 key")
                    return False
                
                result = self.r.delete(*args)
                self.print_result(result)
            
            elif cmd_name == 'EXISTS':
                if len(args) < 1:
                    print("错误: EXISTS 需要至少一个 key")
                    return False
                
                result = self.r.exists(*args)
                self.print_result(result)
            
            elif cmd_name == 'EXPIRE':
                if len(args) != 2:
                    print("错误: EXPIRE 格式: EXPIRE key seconds")
                    return False
                
                key, seconds_str = args
                try:
                    seconds = int(seconds_str)
                except ValueError:
                    print("错误: seconds 必须是整数")
                    return False
                
                result = self.r.expire(key, seconds)
                self.print_result(result)
            
            elif cmd_name == 'PERSIST':
                if len(args) != 1:
                    print("错误: PERSIST 格式: PERSIST key")
                    return False
                
                key = args[0]
                result = self.r.persist(key)
                self.print_result(result)
            
            elif cmd_name == 'TTL':
                if len(args) != 1:
                    print("错误: TTL 格式: TTL key")
                    return False
                
                key = args[0]
                result = self.r.ttl(key)
                self.print_result(result)
            
            elif cmd_name == 'EXPIRETIME':
                if len(args) != 1:
                    print("错误: EXPIRETIME 格式: EXPIRETIME key")
                    return False
                
                key = args[0]
                result = self.r.expiretime(key)
                self.print_result(result)
            
            elif cmd_name == 'SELECT':
                if len(args) != 1:
                    print("错误: SELECT 格式: SELECT db")
                    return False
                
                try:
                    db = int(args[0])
                except ValueError:
                    print("错误: db 必须是整数")
                    return False
                
                try:
                    result = self.r.select(db)
                    self.print_result(result)
                    self.reset_transaction_state()
                    self.watched_keys.clear()
                except Exception as e:
                    print(f"(error) {e}")
            
            elif cmd_name == 'SHUTDOWN':
                print("⚠️  正在关闭 Redis 服务器...")
                try:
                    self.r.shutdown()
                    print("Redis 服务器已关闭")
                    return True
                except redis.ConnectionError:
                    print("Redis 服务器已关闭")
                    return True
                except Exception as e:
                    print(f"(error) {e}")
            
            elif cmd_name == 'WATCH':
                if len(args) < 1:
                    print("错误: WATCH 需要至少一个 key")
                    return False
                
                if self.in_transaction:
                    print("(error) ERR WATCH 不能在 MULTI 之后调用")
                    return False
                
                self.handle_watch(args)
            
            elif cmd_name == 'UNWATCH':
                if self.in_transaction:
                    print("(error) ERR UNWATCH 不能在 MULTI 之后调用（请使用 DISCARD）")
                    return False
                
                self.handle_unwatch()
            
            elif cmd_name == 'MULTI':
                self.handle_multi()
            
            elif cmd_name == 'DISCARD':
                if self.in_transaction:
                    self.handle_discard()
                else:
                    print("(error) ERR DISCARD without MULTI")
            
            elif cmd_name == 'EXEC':
                if self.in_transaction:
                    self.handle_exec()
                else:
                    print("(error) ERR EXEC without MULTI")
            
            elif cmd_name == 'QUIT':
                print("再见！")
                return True
            
            elif cmd_name == 'PING':
                result = self.r.ping()
                self.print_result(result)
            
            elif cmd_name == 'ECHO' and len(args) == 1:
                result = self.r.echo(args[0])
                self.print_result(result)
            
            # ========== List 命令 ==========
            elif cmd_name == 'LPUSH':
                if len(args) < 2:
                    print("错误: LPUSH 格式: LPUSH key value [value ...]")
                    return False
                key = args[0]
                values = args[1:]
                result = self.r.lpush(key, *values)
                self.print_result(result)
            
            elif cmd_name == 'RPUSH':
                if len(args) < 2:
                    print("错误: RPUSH 格式: RPUSH key value [value ...]")
                    return False
                key = args[0]
                values = args[1:]
                result = self.r.rpush(key, *values)
                self.print_result(result)
            
            elif cmd_name == 'LPOP':
                if len(args) != 1:
                    print("错误: LPOP 格式: LPOP key")
                    return False
                key = args[0]
                result = self.r.lpop(key)
                self.print_result(result)
            
            elif cmd_name == 'RPOP':
                if len(args) != 1:
                    print("错误: RPOP 格式: RPOP key")
                    return False
                key = args[0]
                result = self.r.rpop(key)
                self.print_result(result)
            
            elif cmd_name == 'LLEN':
                if len(args) != 1:
                    print("错误: LLEN 格式: LLEN key")
                    return False
                key = args[0]
                result = self.r.llen(key)
                self.print_result(result)
            
            elif cmd_name == 'LINDEX':
                if len(args) != 2:
                    print("错误: LINDEX 格式: LINDEX key index")
                    return False
                key = args[0]
                try:
                    index = int(args[1])
                except ValueError:
                    print("错误: index 必须是整数")
                    return False
                result = self.r.lindex(key, index)
                self.print_result(result)
            
            elif cmd_name == 'LSET':
                if len(args) != 3:
                    print("错误: LSET 格式: LSET key index value")
                    return False
                key = args[0]
                try:
                    index = int(args[1])
                except ValueError:
                    print("错误: index 必须是整数")
                    return False
                value = args[2]
                result = self.r.lset(key, index, value)
                self.print_result(result)
            
            elif cmd_name == 'LRANGE':
                if len(args) != 3:
                    print("错误: LRANGE 格式: LRANGE key start end")
                    return False
                key = args[0]
                try:
                    start = int(args[1])
                    end = int(args[2])
                except ValueError:
                    print("错误: start 和 end 必须是整数")
                    return False
                result = self.r.lrange(key, start, end)
                self.print_result(result)
            
            elif cmd_name == 'LREM':
                if len(args) != 3:
                    print("错误: LREM 格式: LREM key count value")
                    return False
                key = args[0]
                try:
                    count = int(args[1])
                except ValueError:
                    print("错误: count 必须是整数")
                    return False
                value = args[2]
                result = self.r.lrem(key, count, value)
                self.print_result(result)
            
            elif cmd_name == 'LINSERT':
                if len(args) != 4:
                    print("错误: LINSERT 格式: LINSERT key BEFORE|AFTER pivot value")
                    return False
                key = args[0]
                position = args[1].upper()
                if position not in ['BEFORE', 'AFTER']:
                    print("错误: 位置必须是 BEFORE 或 AFTER")
                    return False
                pivot = args[2]
                value = args[3]
                result = self.r.linsert(key, position, pivot, value)
                self.print_result(result)
            
            # ========== Hash 命令 ==========
            elif cmd_name == 'HSET':
                if len(args) != 3:
                    print("错误: HSET 格式: HSET key field value")
                    return False
                key = args[0]
                field = args[1]
                value = args[2]
                result = self.r.hset(key, field, value)
                self.print_result(result)
            
            elif cmd_name == 'HGET':
                if len(args) != 2:
                    print("错误: HGET 格式: HGET key field")
                    return False
                key = args[0]
                field = args[1]
                result = self.r.hget(key, field)
                self.print_result(result)
            
            elif cmd_name == 'HDEL':
                if len(args) < 2:
                    print("错误: HDEL 格式: HDEL key field [field ...]")
                    return False
                key = args[0]
                fields = args[1:]
                result = self.r.hdel(key, *fields)
                self.print_result(result)
            
            elif cmd_name == 'HEXISTS':
                if len(args) != 2:
                    print("错误: HEXISTS 格式: HEXISTS key field")
                    return False
                key = args[0]
                field = args[1]
                result = self.r.hexists(key, field)
                self.print_result(result)
            
            elif cmd_name == 'HLEN':
                if len(args) != 1:
                    print("错误: HLEN 格式: HLEN key")
                    return False
                key = args[0]
                result = self.r.hlen(key)
                self.print_result(result)
            
            elif cmd_name == 'HGETALL': # 不支持
                if len(args) != 1:
                    print("错误: HGETALL 格式: HGETALL key")
                    return False
                key = args[0]
                result = self.r.hgetall(key)
                self.print_result(result)
            
            # ========== Set 命令 ==========
            elif cmd_name == 'SADD':
                if len(args) < 2:
                    print("错误: SADD 格式: SADD key member [member ...]")
                    return False
                key = args[0]
                members = args[1:]
                result = self.r.sadd(key, *members)
                self.print_result(result)
            
            elif cmd_name == 'SREM':
                if len(args) < 2:
                    print("错误: SREM 格式: SREM key member [member ...]")
                    return False
                key = args[0]
                members = args[1:]
                result = self.r.srem(key, *members)
                self.print_result(result)
            
            elif cmd_name == 'SMEMBERS':
                if len(args) != 1:
                    print("错误: SMEMBERS 格式: SMEMBERS key")
                    return False
                key = args[0]
                result = self.r.smembers(key)
                self.print_result(result)
            
            elif cmd_name == 'SCARD':
                if len(args) != 1:
                    print("错误: SCARD 格式: SCARD key")
                    return False
                key = args[0]
                result = self.r.scard(key)
                self.print_result(result)
            
            elif cmd_name == 'SISMEMBER':
                if len(args) != 2:
                    print("错误: SISMEMBER 格式: SISMEMBER key member")
                    return False
                key = args[0]
                member = args[1]
                result = self.r.sismember(key, member)
                self.print_result(result)
            
            # ========== ZSet 命令 ==========
            elif cmd_name == 'ZADD':
                if len(args) < 3 or len(args) % 2 != 1:
                    print("错误: ZADD 格式: ZADD key score member [score member ...]")
                    return False
                key = args[0]
                score_member_pairs = []
                for i in range(1, len(args), 2):
                    try:
                        score = float(args[i])
                    except ValueError:
                        print(f"错误: '{args[i]}' 不是有效的数字")
                        return False
                    member = args[i+1]
                    score_member_pairs.extend([score, member])
                result = self.r.execute_command('ZADD', key, *score_member_pairs)
                self.print_result(result)
            
            elif cmd_name == 'ZCARD':
                if len(args) != 1:
                    print("错误: ZCARD 格式: ZCARD key")
                    return False
                key = args[0]
                result = self.r.execute_command('ZCARD', key)
                self.print_result(result)
            
            elif cmd_name == 'ZSCORE':
                if len(args) != 2:
                    print("错误: ZSCORE 格式: ZSCORE key member")
                    return False
                key = args[0]
                member = args[1]
                result = self.r.execute_command('ZSCORE', key, member)
                self.print_result(result)
            
            elif cmd_name == 'ZREM':
                if len(args) < 2:
                    print("错误: ZREM 格式: ZREM key member [member ...]")
                    return False
                key = args[0]
                members = args[1:]
                result = self.r.execute_command('ZREM', key, *members)
                self.print_result(result)
            
            elif cmd_name == 'ZRANK':
                if len(args) != 2:
                    print("错误: ZRANK 格式: ZRANK key member")
                    return False
                key = args[0]
                member = args[1]
                result = self.r.execute_command('ZRANK', key, member)
                self.print_result(result)
            
            elif cmd_name == 'ZREVRANK':
                if len(args) != 2:
                    print("错误: ZREVRANK 格式: ZREVRANK key member")
                    return False
                key = args[0]
                member = args[1]
                result = self.r.execute_command('ZREVRANK', key, member)
                self.print_result(result)
            
            elif cmd_name == 'ZRANGE':
                if len(args) != 3:
                    print("错误: ZRANGE 格式: ZRANGE key start end")
                    return False
                key = args[0]
                try:
                    start = int(args[1])
                    end = int(args[2])
                except ValueError:
                    print("错误: start 和 end 必须是整数")
                    return False
                result = self.r.execute_command('ZRANGE', key, start, end)
                self.print_result(result)
            
            elif cmd_name == 'ZREVRANGE':
                if len(args) != 3:
                    print("错误: ZREVRANGE 格式: ZREVRANGE key start end")
                    return False
                key = args[0]
                try:
                    start = int(args[1])
                    end = int(args[2])
                except ValueError:
                    print("错误: start 和 end 必须是整数")
                    return False
                result = self.r.execute_command('ZREVRANGE', key, start, end)
                self.print_result(result)
            
            else:
                print(f"错误: 未知命令 '{cmd_name}'")
                self.show_help()
            
            return False
            
        except Exception as e:
            print(f"(error) {e}")
            return False
    
    def show_help(self):
        """显示帮助信息"""
        print("\n支持的命令:")
        print("  基本命令: SET, GET, DEL, MSET, MGET, EXISTS")
        print("  过期命令: EXPIRE, PERSIST, TTL, EXPIRETIME")
        print("  事务命令: MULTI, EXEC, DISCARD, WATCH, UNWATCH")
        print("  列表命令: LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LSET, LRANGE, LREM, LINSERT")
        print("  哈希命令: HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL")
        print("  集合命令: SADD, SREM, SMEMBERS, SCARD, SISMEMBER")
        print("  有序集合命令: ZADD, ZCARD, ZSCORE, ZREM, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE")
        print("  其他命令: SELECT, SHUTDOWN, QUIT, PING, ECHO")
        print("\n事务使用示例:")
        print("  > WATCH balance")
        print("  OK")
        print("  > GET balance")
        print("  100")
        print("  > MULTI")
        print("  OK")
        print("  > SET balance 90")
        print("  QUEUED")
        print("  > EXEC")
        print("  1. OK")
        print()
    
    def run(self):
        """主运行循环"""
        if not self.connect():
            sys.exit(1)
        
        print("Redis 客户端已启动")
        print("输入 'HELP' 查看命令帮助，'QUIT' 退出\n")
        
        while True:
            try:
                if self.in_transaction:
                    prompt = "redis(MULTI)> "
                elif self.watched_keys:
                    prompt = f"redis(WATCH {len(self.watched_keys)})> "
                else:
                    prompt = "redis> "
                
                cmd_input = input(prompt).strip()
                if not cmd_input:
                    continue
                
                cmd_name, args = self.parse_command(cmd_input)
                
                if cmd_name == 'HELP':
                    self.show_help()
                    continue
                
                if self.in_transaction:
                    should_exit = self.handle_transaction_command(cmd_name, args)
                else:
                    should_exit = self.handle_normal_command(cmd_name, args)
                
                if should_exit:
                    break
                
            except KeyboardInterrupt:
                print("\n再见！")
                break
            except Exception as e:
                print(f"(error) {e}")

def main():
    client = RedisClientWithTransaction()
    client.run()

if __name__ == "__main__":
    main()