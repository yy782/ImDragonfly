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
    print("支持命令: SET key value, GET key, DEL key, MSET, MGET, SHUTDOWN, 或输入 'quit' 退出\n")
    
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
                keys = parts[1:]  # 获取所有要删除的键
                result = r.delete(*keys)  # 使用 *keys 展开参数
                if result == 0:
                    print("(nil) - 没有键被删除（所有键都不存在）")
                else:
                    print(f"✓ OK - 已删除 {result} 个键")
            
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
                print("      DEL key")
                print("      SHUTDOWN")
                
        except KeyboardInterrupt:
            print("\n\n再见！")
            break
        except Exception as e:
            print(f"错误: {e}")

if __name__ == "__main__":
    main()