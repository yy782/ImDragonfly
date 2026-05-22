# python3 redis1.py
# cd test/redis-py
import redis
import sys

def main():
    # 连接 Redis（默认配置）
    try:
        r = redis.Redis(
            host='localhost',  # Redis 服务器地址
            port=6379,         # Redis 端口
            db=0,              # 数据库编号
            decode_responses=True,  # 自动解码为字符串
            protocol=2,
            socket_connect_timeout=5,
        )
        
        # 测试连接
        r.ping()
        print("✓ 成功连接到 Redis\n")
        
    except redis.ConnectionError:
        print("✗ 连接失败: 无法连接到 Redis 服务器")
        print("  请确保 Redis 正在运行: redis-server")
        sys.exit(1)
    
    # 命令行交互
    print("Redis 客户端已启动")
    print("支持命令: SET, GET, DEL, MSET, MGET, EXISTS, EXPIRE, PERSIST, TTL, EXPIRETIME, SELECT, SHUTDOWN, 或输入 'quit' 退出\n")
    
    while True:
        try:
            cmd = input("redis> ").strip()
            if not cmd:
                continue
            
            if cmd.lower() == 'quit':
                print("再见！")
                break
            
            # 解析命令
            parts = cmd.split()
            if not parts:
                continue
            
            command = parts[0].upper()
            
            if command == 'SET' and len(parts) == 3:
                key = parts[1]
                value = parts[2]
                result = r.set(key, value)
                print(f"✓ OK - 已设置 {key} = {value}")
                
            elif command == 'GET' and len(parts) == 2:
                key = parts[1]
                value = r.get(key)
                if value is None:
                    print("(nil)")
                else:
                    print(f"{value}")
                    
            elif command == 'MSET':
                if len(parts) < 3 or len(parts) % 2 == 0:
                    print("错误: MSET 需要 key-value 对，格式: MSET key1 value1 key2 value2 ...")
                    continue
                
                # 构建参数字典
                kv_pairs = {}
                for i in range(1, len(parts), 2):
                    key = parts[i]
                    value = parts[i+1]
                    kv_pairs[key] = value
                
                # 执行 MSET
                result = r.mset(kv_pairs)
                if result:
                    print(f"✓ OK - 已设置 {len(kv_pairs)} 个键值对")
                else:
                    print("✗ MSET 执行失败")
            
            elif command == 'MGET':
                if len(parts) < 2:
                    print("错误: MGET 需要至少一个 key，格式: MGET key1 key2 ...")
                    continue
                
                keys = parts[1:]
                values = r.mget(keys)
                
                # 输出结果
                for key, value in zip(keys, values):
                    if value is None:
                        print(f"{key}: (nil)")
                    else:
                        print(f"{key}: {value}")
                        
            elif command == 'DEL':
                if len(parts) < 2:
                    print("错误: DEL 需要至少一个 key，格式: DEL key1 [key2 key3 ...]")
                    continue
                keys = parts[1:]
                result = r.delete(*keys)
                if result == 0:
                    print("(nil) - 没有键被删除（所有键都不存在）")
                else:
                    print(f"✓ OK - 已删除 {result} 个键")
            
            elif command == 'EXISTS':
                if len(parts) < 2:
                    print("错误: EXISTS 需要至少一个 key，格式: EXISTS key1 [key2 ...]")
                    continue
                keys = parts[1:]
                result = r.exists(*keys)
                print(f"(integer) {result}")
            
            elif command == 'EXPIRE':
                if len(parts) != 3:
                    print("错误: EXPIRE 格式: EXPIRE key seconds")
                    continue
                key = parts[1]
                try:
                    seconds = int(parts[2])
                except ValueError:
                    print("错误: seconds 必须是整数")
                    continue
                result = r.expire(key, seconds)
                if result:
                    print(f"(integer) 1")
                else:
                    print(f"(integer) 0")
            
            elif command == 'PERSIST':
                if len(parts) != 2:
                    print("错误: PERSIST 格式: PERSIST key")
                    continue
                key = parts[1]
                result = r.persist(key)
                if result:
                    print(f"(integer) 1")
                else:
                    print(f"(integer) 0")
            
            elif command == 'TTL':
                if len(parts) != 2:
                    print("错误: TTL 格式: TTL key")
                    continue
                key = parts[1]
                result = r.ttl(key)
                print(f"(integer) {result}")
            
            elif command == 'EXPIRETIME':
                if len(parts) != 2:
                    print("错误: EXPIRETIME 格式: EXPIRETIME key")
                    continue
                key = parts[1]
                result = r.expiretime(key)
                print(f"(integer) {result}")
            
            elif command == 'SELECT':
                if len(parts) != 2:
                    print("错误: SELECT 格式: SELECT db")
                    continue
                try:
                    db = int(parts[1])
                except ValueError:
                    print("错误: db 必须是整数")
                    continue
                try:
                    r.select(db)
                    print(f"✓ OK - 已切换到数据库 {db}")
                except Exception as e:
                    print(f"错误: {e}")
            
            elif command == 'SHUTDOWN':
                print("⚠️  正在关闭 Redis 服务器...")
                try:
                    r.shutdown()
                    print("✓ Redis 服务器已关闭")
                    print("再见！")
                    break
                except redis.ConnectionError:
                    # shutdown 命令会断开连接，这是正常的
                    print("✓ Redis 服务器已关闭")
                    print("再见！")
                    break
                except Exception as e:
                    print(f"✗ 关闭失败: {e}")
            
            else:
                print("错误: 无效命令")
                print("用法: SET key value")
                print("      GET key")
                print("      DEL key [key ...]")
                print("      MSET key value [key value ...]")
                print("      MGET key [key ...]")
                print("      EXISTS key [key ...]")
                print("      EXPIRE key seconds")
                print("      PERSIST key")
                print("      TTL key")
                print("      EXPIRETIME key")
                print("      SELECT db")
                print("      SHUTDOWN")
                
        except KeyboardInterrupt:
            print("\n\n再见！")
            break
        except Exception as e:
            print(f"错误: {e}")

if __name__ == "__main__":
    main()