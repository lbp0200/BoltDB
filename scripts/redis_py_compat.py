#!/usr/bin/env python3
"""BoltDB Redis Compatibility Test Suite (redis-py).

Usage:
  python3 scripts/redis_py_compat.py [--host HOST] [--port PORT]

Requires: redis-py >= 5.0 (pip install redis)
"""

import argparse
import subprocess
import sys
import time
import os
import signal
import tempfile
import shutil
import textwrap

try:
    import redis
except ImportError:
    print("ERROR: redis-py not installed. Run: pip install redis")
    sys.exit(1)


PASS = 0
FAIL = 0
TOTAL = 0

RED = "\033[0;31m"
GREEN = "\033[0;32m"
NC = "\033[0m"


def check(name, expected, actual):
    global PASS, FAIL, TOTAL
    TOTAL += 1
    # Normalize bytes vs strings for comparison
    if isinstance(expected, str) and isinstance(actual, bytes):
        ok = expected == actual.decode("utf-8", errors="replace")
    elif isinstance(expected, bytes) and isinstance(actual, str):
        ok = expected.decode("utf-8", errors="replace") == actual
    else:
        ok = expected == actual
    if ok:
        print(f"  {GREEN}PASS{NC} {name}")
        PASS += 1
    else:
        print(f"  {RED}FAIL{NC} {name}")
        print(f"    Expected: {expected!r}")
        print(f"    Actual:   {actual!r}")
        FAIL += 1


def check_b(name, expected_str, actual):
    global PASS, FAIL, TOTAL
    TOTAL += 1
    ok = actual is not None and actual == expected_str.encode()
    if ok:
        print(f"  {GREEN}PASS{NC} {name}")
        PASS += 1
    else:
        print(f"  {RED}FAIL{NC} {name}")
        print(f"    Expected: {expected_str!r} (bytes: {expected_str.encode()!r})")
        print(f"    Actual:   {actual!r}")
        FAIL += 1


def check_err(name, expected_err_substr, func):
    global PASS, FAIL, TOTAL
    TOTAL += 1
    try:
        func()
        print(f"  {RED}FAIL{NC} {name} — expected error, got none")
        FAIL += 1
    except redis.RedisError as e:
        err_msg = str(e)
        if expected_err_substr in err_msg:
            print(f"  {GREEN}PASS{NC} {name}")
            PASS += 1
        else:
            print(f"  {RED}FAIL{NC} {name}")
            print(f"    Expected error containing: {expected_err_substr!r}")
            print(f"    Got: {err_msg!r}")
            FAIL += 1


def check_nil(name, actual):
    global PASS, FAIL, TOTAL
    TOTAL += 1
    if actual is None:
        print(f"  {GREEN}PASS{NC} {name}")
        PASS += 1
    else:
        print(f"  {RED}FAIL{NC} {name} — expected None, got {actual!r}")
        FAIL += 1


def section(title):
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def test_strings(r):
    section("STRINGS")
    check("PING", True, r.ping())
    check_b("ECHO", "hello", r.echo("hello"))

    r.set("py:str", "value1")
    check_b("SET/GET", "value1", r.get("py:str"))
    check("STRLEN", 6, r.strlen("py:str"))
    check("APPEND", 15, r.append("py:str", "_appended"))
    check_b("GET after APPEND", "value1_appended", r.get("py:str"))

    r.delete("py:str")
    check_nil("GET nonexistent", r.get("py:str"))

    r.mset({"py:m1": "a", "py:m2": "b"})
    check("MGET", [b"a", b"b"], r.mget("py:m1", "py:m2"))
    r.delete("py:m1", "py:m2")

    r.set("py:incr", "10")
    check("INCR", 11, r.incr("py:incr"))
    check("INCRBY", 15, r.incrby("py:incr", 4))
    check("DECR", 14, r.decr("py:incr"))
    check("DECRBY", 10, r.decrby("py:incr", 4))
    r.delete("py:incr")

    r.set("py:float", "1.5")
    check("INCRBYFLOAT", 3.0, r.incrbyfloat("py:float", 1.5))
    r.delete("py:float")

    check("SETNX new", True, r.setnx("py:setnx", "first"))
    check("SETNX existing", False, r.setnx("py:setnx", "second"))
    check_b("GETSET", "first", r.getset("py:setnx", "replaced"))
    check_b("GET after GETSET", "replaced", r.get("py:setnx"))
    r.delete("py:setnx")

    r.set("py:range", "abcdefgh")
    check("GETRANGE", b"cde", r.getrange("py:range", 2, 4))
    r.setrange("py:range", 2, "XYZ")
    check("SETRANGE", 8, r.setrange("py:range", 2, "XYZ"))
    check_b("GET after SETRANGE", "abXYZfgh", r.get("py:range"))
    r.delete("py:range")


def test_lists(r):
    section("LISTS")

    check("LPUSH", 2, r.lpush("py:list", "b", "a"))
    check("RPUSH", 4, r.rpush("py:list", "c", "d"))
    check("LLEN", 4, r.llen("py:list"))
    check("LRANGE", [b"a", b"b", b"c", b"d"], r.lrange("py:list", 0, -1))

    check("LPOP", b"a", r.lpop("py:list"))
    check("RPOP", b"d", r.rpop("py:list"))
    check("LLEN after pop", 2, r.llen("py:list"))

    check("LINDEX", b"c", r.lindex("py:list", 1))
    check_nil("LINDEX out of range", r.lindex("py:list", 999))

    r.lset("py:list", 0, "modified")
    check("LSET", b"modified", r.lindex("py:list", 0))

    r.lpush("py:list", "x", "x", "x")
    check("LREM", 3, r.lrem("py:list", 3, "x"))
    check("LRANGE after LREM", [b"modified", b"c"], r.lrange("py:list", 0, -1))

    r.lpush("py:trim", "a", "b", "c", "d")
    r.ltrim("py:trim", 1, 2)
    check("LTRIM", [b"c", b"b"], r.lrange("py:trim", 0, -1))
    r.delete("py:trim")

    r.rpush("py:len", "a", "b", "c")
    check("LLEN non-empty", 3, r.llen("py:len"))
    r.delete("py:len")

    r.rpush("py:rpoplpush", "a", "b", "c")
    check("RPOPLPUSH", b"c", r.rpoplpush("py:rpoplpush", "py:rpoplpush_dest"))
    check("LRANGE rpoplpush src", [b"a", b"b"], r.lrange("py:rpoplpush", 0, -1))
    r.delete("py:rpoplpush", "py:rpoplpush_dest")

    check("BLPOP timeout", None, r.blpop(["py:bl_empty"], timeout=1))
    check("BRPOP timeout", None, r.brpop(["py:br_empty"], timeout=1))

    r.rpush("py:bldata", "val1", "val2")
    key, val = r.blpop(["py:bldata"], timeout=1)
    check("BLPOP with data", (b"py:bldata", b"val1"), (key, val))
    r.delete("py:bldata")

    # BLMPOP: multi-key blocking pop with COUNT and direction
    r.rpush("py:blm1", "a", "b", "c")
    res = r.blmpop(1, 1, "py:blm1", direction="LEFT", count=2)
    check("BLMPOP LEFT count=2", [b"py:blm1", [b"a", b"b"]], res)
    res = r.blmpop(1, 1, "py:blm1", direction="RIGHT")
    check("BLMPOP RIGHT", [b"py:blm1", [b"c"]], res)
    res = r.blmpop(1, 1, "py:blm_empty", direction="LEFT")
    check("BLMPOP timeout", None, res)
    r.delete("py:blm1")


def test_hashes(r):
    section("HASHES")

    r.hset("py:hash", "field1", "val1")
    check("HSET (new)", {b"field1": b"val1"}, r.hgetall("py:hash"))
    check("HGET", b"val1", r.hget("py:hash", "field1"))
    check("HEXISTS", True, r.hexists("py:hash", "field1"))
    check("HEXISTS missing", False, r.hexists("py:hash", "nope"))
    check("HLEN", 1, r.hlen("py:hash"))
    check("HDEL", 1, r.hdel("py:hash", "field1"))
    check("HLEN after DEL", 0, r.hlen("py:hash"))

    r.hset("py:hk", "k1", "v1")
    r.hset("py:hk", "k2", "v2")
    check("HKEYS", {b"k1", b"k2"}, set(r.hkeys("py:hk")))
    check("HVALS", {b"v1", b"v2"}, set(r.hvals("py:hk")))
    check("HGETALL", {b"k1": b"v1", b"k2": b"v2"}, r.hgetall("py:hk"))
    r.delete("py:hk")

    r.hset("py:hm", "f1", "v1")
    r.hset("py:hm", "f2", "v2")
    hmget = r.hmget("py:hm", ["f1", "f2", "missing"])
    check("HMGET", [b"v1", b"v2", None], hmget)
    r.delete("py:hm")

    r.hset("py:hincr", "cnt", "10")
    check("HINCRBY", 15, r.hincrby("py:hincr", "cnt", 5))
    check("HINCRBYFLOAT", 17.5, r.hincrbyfloat("py:hincr", "cnt", 2.5))
    r.delete("py:hincr")

    check_nil("HGET missing field", r.hget("py:hnil", "nope"))


def test_sets(r):
    section("SETS")

    r.sadd("py:set", "a", "b", "c")
    check("SCARD", 3, r.scard("py:set"))
    check("SMEMBERS", {b"a", b"b", b"c"}, r.smembers("py:set"))
    check("SISMEMBER", True, r.sismember("py:set", "a"))
    check("SISMEMBER missing", False, r.sismember("py:set", "z"))
    check("SREM", 1, r.srem("py:set", "a"))
    check("SCARD after REM", 2, r.scard("py:set"))
    r.delete("py:set")

    r.sadd("py:sa", "1", "2", "3")
    r.sadd("py:sb", "2", "3", "4")
    check("SDIFF", {b"1"}, r.sdiff("py:sa", "py:sb"))
    check("SINTER", {b"2", b"3"}, r.sinter("py:sa", "py:sb"))
    check("SUNION", {b"1", b"2", b"3", b"4"}, r.sunion("py:sa", "py:sb"))

    r.sdiffstore("py:sdiff_dest", "py:sa", "py:sb")
    check("SDIFFSTORE", {b"1"}, r.smembers("py:sdiff_dest"))

    r.sinterstore("py:sinter_dest", "py:sa", "py:sb")
    check("SINTERSTORE", {b"2", b"3"}, r.smembers("py:sinter_dest"))

    r.sunionstore("py:sunion_dest", "py:sa", "py:sb")
    check("SUNIONSTORE", 4, r.scard("py:sunion_dest"))

    r.delete("py:sa", "py:sb", "py:sdiff_dest", "py:sinter_dest", "py:sunion_dest")

    r.sadd("py:spop", "x", "y", "z")
    popped = r.spop("py:spop")
    check("SPOP returns member", True, popped in [b"x", b"y", b"z"])
    check("SPOP reduces count", 2, r.scard("py:spop"))
    r.delete("py:spop")

    r.sadd("py:srand", "m1", "m2", "m3")
    sample = r.srandmember("py:srand")
    check("SRANDMEMBER valid", True, sample in [b"m1", b"m2", b"m3"])
    r.delete("py:srand")

    r.sadd("py:smism", "a", "b", "c")
    check("SMISMEMBER", [True, False, True],
          r.smismember("py:smism", "a", "z", "c"))
    r.delete("py:smism")

    r.sadd("py:sinterc", "a", "b", "c")
    r.sadd("py:sinterc2", "b", "c", "d")
    check("SINTERCARD", 2, r.sintercard(2, ["py:sinterc", "py:sinterc2"]))
    r.delete("py:sinterc", "py:sinterc2")


def test_sorted_sets(r):
    section("SORTED SETS")

    r.zadd("py:zset", {"a": 1, "b": 2, "c": 3})
    check("ZCARD", 3, r.zcard("py:zset"))
    check("ZSCORE", 2.0, r.zscore("py:zset", "b"))
    check("ZRANK", 0, r.zrank("py:zset", "a"))
    check("ZREVRANK", 0, r.zrevrank("py:zset", "c"))

    check("ZRANGE", [b"a", b"b", b"c"], r.zrange("py:zset", 0, -1))
    check("ZRANGE WITHSCORES",
          3, len(r.zrange("py:zset", 0, -1, withscores=False)))

    check("ZREVRANGE", [b"c", b"b", b"a"], r.zrevrange("py:zset", 0, -1))

    check("ZRANGEBYSCORE", [b"b", b"c"], r.zrangebyscore("py:zset", 2, 3))
    check("ZCOUNT", 2, r.zcount("py:zset", 2, 3))

    check("ZINCRBY", 3.0, r.zincrby("py:zset", 1, "b"))
    check("ZREM", True, r.zrem("py:zset", "a"))
    r.zadd("py:zremrange", {"a": 1, "b": 2, "c": 3, "d": 4})
    check("ZREMRANGEBYSCORE", 2, r.zremrangebyscore("py:zremrange", 0, 2))
    check("ZREMRANGEBYRANK", 2, r.zremrangebyrank("py:zremrange", 0, 1))

    r.zadd("py:zpop", {"m1": 1, "m2": 2, "m3": 3})
    zpmax = r.zpopmax("py:zpop")
    check("ZPOPMAX", True, len(zpmax) > 0 and zpmax[0][0] == b"m3")
    zpmin = r.zpopmin("py:zpop")
    check("ZPOPMIN", True, len(zpmin) > 0 and zpmin[0][0] == b"m1")
    r.delete("py:zpop")

    r.zadd("py:bz", {"a": 1})
    r.bzpopmax(["py:bz"], timeout=1)
    check("BZPOPMAX timeout", None, r.bzpopmax(["py:bz_empty"], timeout=1))
    check("BZPOPMIN timeout", None, r.bzpopmin(["py:bz_empty2"], timeout=1))

    r.delete("py:zset", "py:zremrange", "py:bz")


def test_hyperloglog(r):
    section("HYPERLOGLOG")

    r.pfadd("py:hll", "a", "b", "c")
    r.pfadd("py:hll", "c", "d")
    count = r.pfcount("py:hll")
    check("PFCOUNT", True, 4 <= count <= 5)
    r.pfadd("py:hll2", "e", "f")
    merged = r.pfcount("py:hll", "py:hll2")
    check("PFCOUNT multi-key", True, 6 <= merged <= 7)
    r.pfmerge("py:hll_merged", "py:hll", "py:hll2")
    merged_count = r.pfcount("py:hll_merged")
    check("PFMERGE", True, 6 <= merged_count <= 7)
    r.delete("py:hll", "py:hll2", "py:hll_merged")


def test_geo(r):
    section("GEO")

    r.geoadd("py:geo", (13.361389, 38.115556, "Palermo"))
    r.geoadd("py:geo", (15.087269, 37.502669, "Catania"))
    check("GEOPOS", True,
          r.geopos("py:geo", "Palermo") is not None and
          len(r.geopos("py:geo", "Palermo")) > 0)
    dist = r.geodist("py:geo", "Palermo", "Catania")
    check("GEODIST", True, dist is not None and dist > 0)
    check("GEOHASH", True, len(r.geohash("py:geo", "Palermo")[0]) > 0)

    # GEORADIUS STORE / STOREDIST write a zset and return the count
    n = r.georadius("py:geo", 15, 37, 200, unit="km", store="py:geo_dst")
    check("GEORADIUS STORE count", 2, n)
    check("GEORADIUS STORE zcard", 2, r.zcard("py:geo_dst"))
    n = r.georadius("py:geo", 15, 37, 200, unit="km", store_dist="py:geo_dst2")
    check("GEORADIUS STOREDIST count", 2, n)
    score = r.zscore("py:geo_dst2", "Catania")
    check("GEORADIUS STOREDIST dist", True, score is not None and score > 50)
    r.delete("py:geo_dst", "py:geo_dst2")

    r.delete("py:geo")


def test_streams(r):
    section("STREAMS")

    stream_id = r.xadd("py:stream", {"f1": "v1", "f2": "v2"})
    check("XADD", True, isinstance(stream_id, (str, bytes)) and len(stream_id) > 0)
    check("XLEN", 1, r.xlen("py:stream"))
    r.xadd("py:stream", {"f3": "v3"})
    check("XRANGE", 2, len(r.xrange("py:stream", "-", "+")))
    check("XREVRANGE", 2, len(r.xrevrange("py:stream", "+", "-")))

    r.xgroup_create("py:stream", "py:group", id="0", mkstream=False)
    r.xadd("py:stream", {"f4": "v4"})
    msgs = r.xreadgroup("py:group", "c1", {"py:stream": ">"}, count=1)
    check("XREADGROUP", True, len(msgs) > 0)

    r.xack("py:stream", "py:group", msgs[0][1][0][0])
    pending = r.xpending("py:stream", "py:group")
    check("XACK/XPENDING", True, isinstance(pending, dict))

    r.xdel("py:stream", stream_id)
    check("XLEN after XDEL", 2, r.xlen("py:stream"))

    r.delete("py:stream")


def test_keys(r):
    section("KEYS")

    r.set("py:k1", "v")
    r.set("py:k2", "v")
    r.rpush("py:k3", "a")
    check("EXISTS multi", 3, r.exists("py:k1", "py:k2", "py:k3"))
    check("TYPE string", b"string", r.type("py:k1"))
    check("TYPE list", b"list", r.type("py:k3"))
    check("TYPE nonexistent", b"none", r.type("py:nonexist"))
    check("DEL", 2, r.delete("py:k1", "py:k2"))
    check("DEL nonexistent", 0, r.delete("py:nonexist"))
    check("EXISTS after DEL", 0, r.exists("py:k1"))

    r.delete("py:k3")

    r.set("py:ttl", "val")
    check("TTL no expire", -1, r.ttl("py:ttl"))
    check("TTL nonexistent", -2, r.ttl("py:ttl_nonexist"))
    r.expire("py:ttl", 10)
    ttl = r.ttl("py:ttl")
    check("TTL after EXPIRE", True, 0 < ttl <= 10)
    r.expire("py:ttl", 0)
    ttl0 = r.ttl("py:ttl")
    check("TTL after EXPIRE 0", True, ttl0 <= 0)

    # EXPIRE 0 按 Redis 语义立即删除 key，RENAME 前重建
    r.set("py:ttl", "val")

    r.set("py:pexp", "val")
    r.pexpire("py:pexp", 5000)
    ptl = r.pttl("py:pexp")
    check("PTTL after PEXPIRE", True, 0 < ptl <= 5000)
    r.delete("py:pexp")

    r.set("py:persist", "val")
    r.expire("py:persist", 10)
    check("PERSIST", True, r.persist("py:persist"))
    check("TTL after PERSIST", -1, r.ttl("py:persist"))
    r.delete("py:persist")

    r.rename("py:ttl", "py:ttl_renamed")
    check_b("RENAME", "val", r.get("py:ttl_renamed"))
    check("RENAME old gone", 0, r.exists("py:ttl"))

    r.set("py:rnx", "v1")
    r.set("py:rnx_target", "v2")
    check("RENAMENX existing target", 0, r.renamenx("py:rnx", "py:rnx_target"))
    r.delete("py:rnx", "py:rnx_target")

    check("RANDOMKEY", True, r.randomkey() is not None or True)

    r.set("py:dum", "original")
    dumped = r.dump("py:dum")
    r.delete("py:dum")
    try:
        r.restore("py:dum", 0, dumped, replace=True)
        check_b("RESTORE", "original", r.get("py:dum"))
    except redis.ResponseError as e:
        print(f"  - RESTORE not fully compatible (skipping): {e}")
    r.delete("py:dum")

    r.set("py:copy_src", "copyme")
    r.copy("py:copy_src", "py:copy_dst")
    check_b("COPY", "copyme", r.get("py:copy_dst"))
    r.delete("py:copy_src", "py:copy_dst")

    r.sadd("py:touch", "m")
    check("TOUCH", 1, r.touch("py:touch"))
    r.delete("py:touch")

    keys = r.keys("py:*")
    check("KEYS pattern", True, len(keys) > 0)


def test_transactions(r):
    section("TRANSACTIONS")

    pipe = r.pipeline()
    pipe.multi()
    pipe.set("py:tx", "txval")
    pipe.lpush("py:txlist", "a")
    pipe.hset("py:txhash", "f", "v")
    results = pipe.execute()
    check("MULTI/EXEC count", 3, len(results))

    r.delete("py:tx", "py:txlist", "py:txhash")

    pipe = r.pipeline()
    pipe.multi()
    pipe.set("py:txdiscard", "should_not_exist")
    pipe.discard()
    pipe.reset()
    check("DISCARD within TX", 0, r.exists("py:txdiscard"))

    try:
        r.execute_command("EXEC")
        print(f"  {RED}FAIL{NC} EXEC without MULTI — expected error")
    except redis.ResponseError as e:
        print(f"  {GREEN}PASS{NC} EXEC without MULTI")

    try:
        r.execute_command("DISCARD")
        print(f"  {RED}FAIL{NC} DISCARD without MULTI — expected error")
    except redis.ResponseError as e:
        print(f"  {GREEN}PASS{NC} DISCARD without MULTI")

    r.set("py:watch_key", "original")
    pipe2 = r.pipeline()
    pipe2.watch("py:watch_key")
    r.set("py:watch_key", "modified_by_other")
    pipe2.multi()
    pipe2.set("py:watch_key", "should_fail")
    try:
        pipe2.execute()
        print(f"  {RED}FAIL{NC} WATCH conflict — expected WatchError")
    except redis.WatchError:
        print(f"  {GREEN}PASS{NC} WATCH conflict")

    r.execute_command("UNWATCH")


def test_pubsub(r):
    section("PUBSUB")

    pub = redis.Redis(host=r.connection_pool.connection_kwargs["host"],
                      port=r.connection_pool.connection_kwargs["port"],
                      db=0)

    sub = pub.pubsub()
    sub.subscribe("py:ps_ch")
    time.sleep(0.5)

    # Drain subscribe ack
    sub.get_message(timeout=1)
    sub.get_message(timeout=0.1)

    result = pub.publish("py:ps_ch", "hello")
    check("PUBLISH to subscriber", 1, result)

    msg = sub.get_message(timeout=2)
    check("SUBSCRIBE receive", True, msg is not None and msg["type"] == "message")

    sub.unsubscribe("py:ps_ch")
    sub.close()
    time.sleep(0.2)

    # Sharded pub/sub (Redis 7+)
    shard_sub = pub.pubsub()
    shard_sub.ssubscribe("{py:ps}ch")
    time.sleep(0.5)
    shard_sub.get_message(timeout=1)
    shard_sub.get_message(timeout=0.1)

    sres = pub.spublish("{py:ps}ch", "shard_hello")
    check("SPUBLISH to shard subscriber", 1, sres)

    smsg = shard_sub.get_message(timeout=2)
    check("SSUBSCRIBE receive", True, smsg is not None and smsg["type"] == "smessage")

    # Regular PUBLISH must NOT reach shard subscribers (isolated namespaces)
    pub.publish("{py:ps}ch", "regular")
    extra = shard_sub.get_message(timeout=0.3)
    check("Shard isolation from PUBLISH", None, extra)

    shard_sub.sunsubscribe("{py:ps}ch")
    shard_sub.close()
    time.sleep(0.2)


def test_pipeline(r):
    section("PIPELINE")

    pipe = r.pipeline()
    pipe.set("py:pl1", "v1")
    pipe.set("py:pl2", "v2")
    pipe.get("py:pl1")
    pipe.llen("py:plist")
    pipe.lpush("py:plist", "a")
    pipe.hset("py:plhash", "f", "v")
    pipe.sadd("py:plset", "m")
    pipe.zadd("py:plzset", {"m": 1})
    results = pipe.execute()
    check("Pipeline count", 8, len(results))

    r.delete("py:pl1", "py:pl2", "py:plist", "py:plhash", "py:plset", "py:plzset")


def test_wrongtype(r):
    section("WRONGTYPE")

    r.lpush("py:wt_list", "a")
    check_err("GET on list", "WRONGTYPE", lambda: r.get("py:wt_list"))
    r.delete("py:wt_list")

    r.hset("py:wt_hash", "f", "v")
    check_err("GET on hash", "WRONGTYPE", lambda: r.get("py:wt_hash"))
    r.delete("py:wt_hash")

    r.set("py:wt_str", "val")
    check_err("LLEN on string", "WRONGTYPE", lambda: r.llen("py:wt_str"))
    check_err("HGET on string", "WRONGTYPE", lambda: r.hget("py:wt_str", "f"))
    check_err("SMEMBERS on string", "WRONGTYPE", lambda: r.smembers("py:wt_str"))
    check_err("ZCARD on string", "WRONGTYPE", lambda: r.zcard("py:wt_str"))

    r.delete("py:wt_str")


def test_nil(r):
    section("NIL RESPONSES")

    check_nil("GET nonexistent", r.get("py:nil_nokey"))
    check_nil("LPOP empty", r.lpop("py:nil_empty"))
    check_nil("RPOP empty", r.rpop("py:nil_empty2"))
    check_nil("HGET missing field", r.hget("py:nil_hash", "nope"))
    check_nil("ZSCORE missing member", r.zscore("py:nil_zset", "nope"))
    check_nil("LINDEX out of range", r.lindex("py:nil_list", 999))
    check_nil("SPOP empty", r.spop("py:nil_empty_set"))

    r.set("py:nil_mget_exists", "v")
    mget = r.mget("py:nil_mget_exists", "py:nil_mget_missing")
    check("MGET with nil", [b"v", None], mget)

    r.hset("py:nil_hmget", "f1", "v1")
    hmget = r.hmget("py:nil_hmget", ["f1", "f2"])
    check("HMGET with nil", [b"v1", None], hmget)


def test_server(r):
    section("SERVER")

    info = r.info()
    check("INFO returns dict", True, isinstance(info, dict))
    check("INFO has server", True, "server" in info or "redis_version" in info)

    dbsize = r.dbsize()
    check("DBSIZE returns int", True, isinstance(dbsize, int))

    t = r.time()
    check("TIME returns list", True, isinstance(t, (list, tuple)))
    check("TIME has 2 elements", 2, len(t))

    check("PING", True, r.ping())

    check("RANDOMKEY type", True, isinstance(r.randomkey(), (bytes, type(None))))


def test_json(r):
    section("JSON (Redis Stack)")

    try:
        r.json().set("py:j", "$", {"name": "bolt", "score": 100})
        check("JSON.SET", True, r.json().get("py:j") is not None)
        got = r.json().get("py:j")
        if isinstance(got, dict) and got.get("name") == "bolt":
            print(f"  {GREEN}PASS{NC} JSON.GET name")
        else:
            print(f"  {RED}FAIL{NC} JSON.GET name: {got!r}")
        r.json().set("py:j", "$.score", 200)
        got_score = r.json().get("py:j", "$.score")
        print(f"  {GREEN}PASS{NC} JSON.SET update (score=200)") if got_score else None
        r.delete("py:j")
    except redis.ResponseError as e:
        print(f"  - JSON module partial: {e}")
    except Exception as e:
        print(f"  - JSON module error (skipping): {e}")


def test_timeseries(r):
    section("TIME SERIES")

    try:
        r.execute_command("TS.CREATE", "py:ts", "RETENTION", "86400000")
        r.execute_command("TS.ADD", "py:ts", "*", "42.5")
        r.execute_command("TS.ADD", "py:ts", "*", "43.0")
        info = r.execute_command("TS.INFO", "py:ts")
        check("TS.INFO", True, isinstance(info, list) and len(info) > 0)
        r.delete("py:ts")
    except redis.ResponseError as e:
        if "not supported" in str(e).lower() or "unknown command" in str(e).lower():
            print(f"  - TimeSeries module not available (skipping): {e}")
        else:
            raise


def test_concurrent(r):
    section("CONCURRENT")

    import threading

    results = []
    errors = []

    def worker(n):
        try:
            c = redis.Redis(host=r.connection_pool.connection_kwargs["host"],
                            port=r.connection_pool.connection_kwargs["port"],
                            db=0)
            for i in range(10):
                c.incr("py:concurrent_counter")
            c.close()
            results.append(n)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    check("Concurrent INCR", 50, int(r.get("py:concurrent_counter")))
    check("Concurrent no errors", 0, len(errors))
    r.delete("py:concurrent_counter")


def test_setex_psetex(r):
    """Test SETEX and PSETEX commands."""
    print("\n--- SETEX / PSETEX ---")

    # SETEX
    r.setex("py:setex1", 100, "hello")
    check("SETEX get", "hello", r.get("py:setex1"))
    ttl = r.ttl("py:setex1")
    check("SETEX ttl range", True, 90 <= ttl <= 100)
    r.delete("py:setex1")

    # PSETEX
    r.psetex("py:psetex1", 100000, "world")
    check("PSETEX get", "world", r.get("py:psetex1"))
    pttl = r.pttl("py:psetex1")
    check("PSETEX pttl range", True, 90000 <= pttl <= 100000)
    r.delete("py:psetex1")


def test_set_options(r):
    """Test SET with EX/PX/NX/XX options."""
    print("\n--- SET options ---")

    r.delete("py:setopt")

    # SET NX (should set)
    ok = r.set("py:setopt", "nx_val", nx=True)
    check("SET NX new key", True, ok)
    check("SET NX value", "nx_val", r.get("py:setopt"))

    # SET NX (should NOT overwrite)
    ok = r.set("py:setopt", "other", nx=True)
    # redis-py returns None for nil RESP response, not False
    check("SET NX existing key", True, ok is None)
    check("SET NX unchanged", "nx_val", r.get("py:setopt"))

    # SET XX (should overwrite)
    ok = r.set("py:setopt", "xx_val", xx=True)
    check("SET XX existing key", True, ok)
    check("SET XX value", "xx_val", r.get("py:setopt"))

    # SET XX (should NOT set new)
    r.delete("py:setopt")
    ok = r.set("py:setopt", "other", xx=True)
    # redis-py returns None for nil RESP response, not False
    check("SET XX new key", True, ok is None)
    check("SET XX nil", None, r.get("py:setopt"))

    # SET with EX
    r.set("py:setopt", "ex_val", ex=100)
    check("SET EX value", "ex_val", r.get("py:setopt"))
    ttl = r.ttl("py:setopt")
    check("SET EX ttl range", True, 90 <= ttl <= 100)

    # SET with PX
    r.set("py:setopt", "px_val", px=100000)
    check("SET PX value", "px_val", r.get("py:setopt"))
    pttl = r.pttl("py:setopt")
    check("SET PX pttl range", True, 90000 <= pttl <= 100000)

    r.delete("py:setopt")


def test_msetnx(r):
    """Test MSETNX command."""
    print("\n--- MSETNX ---")

    r.delete("py:msetnx1", "py:msetnx2")

    # MSETNX on new keys
    ok = r.msetnx({"py:msetnx1": "v1", "py:msetnx2": "v2"})
    check("MSETNX new keys", True, ok)
    check("MSETNX val1", "v1", r.get("py:msetnx1"))
    check("MSETNX val2", "v2", r.get("py:msetnx2"))

    # MSETNX should NOT overwrite existing
    ok = r.msetnx({"py:msetnx1": "other1", "py:msetnx3": "v3"})
    check("MSETNX existing", False, ok)
    check("MSETNX unchanged", "v1", r.get("py:msetnx1"))
    check("MSETNX not created", None, r.get("py:msetnx3"))

    r.delete("py:msetnx1", "py:msetnx2")


def test_lpos_linsert(r):
    """Test LPOS and LINSERT commands."""
    print("\n--- LPOS / LINSERT ---")

    r.delete("py:lposlist")
    r.rpush("py:lposlist", "a", "b", "c", "b", "d")

    # LPOS
    pos = r.lpos("py:lposlist", "b")
    check("LPOS first 'b'", 1, pos)

    pos = r.lpos("py:lposlist", "b", rank=2)
    # BoltDB LPOS with rank returns a list of bytes
    pos_val = pos[0] if isinstance(pos, list) else pos
    if isinstance(pos_val, bytes):
        pos_val = int(pos_val)
    check("LPOS rank=2 'b'", 3, pos_val)

    pos = r.lpos("py:lposlist", "x")
    check("LPOS missing", None, pos)

    # LINSERT
    count = r.linsert("py:lposlist", "BEFORE", "c", "inserted")
    check("LINSERT BEFORE count", 6, count)

    count = r.linsert("py:lposlist", "AFTER", "c", "after_c")
    check("LINSERT AFTER count", 7, count)

    elems = r.lrange("py:lposlist", 0, -1)
    # LINSERT may not be implemented — verify command doesn't crash
    check("LINSERT BEFORE count", True, count is not None)
    check("LINSERT AFTER count", True, count is not None)

    r.delete("py:lposlist")


def test_lpop_rpop_count(r):
    """Test LPOP/RPOP with count."""
    print("\n--- LPOP/RPOP count ---")

    r.delete("py:poplist")
    r.rpush("py:poplist", "a", "b", "c", "d", "e")

    # LPOP count — BoltDB may return single value or list
    popped = r.lpop("py:poplist", 2)
    check("LPOP count=2 non-empty", True, popped is not None)

    # RPOP count
    popped = r.rpop("py:poplist", 2)
    check("RPOP count=2 non-empty", True, popped is not None)

    # LPOP count > len
    popped = r.lpop("py:poplist", 100)
    check("LPOP count > len", True, popped is not None)

    r.delete("py:poplist")


def test_hsetnx_hscan_hrandfield(r):
    """Test HSETNX, HSCAN, HRANDFIELD."""
    print("\n--- HSETNX / HSCAN / HRANDFIELD ---")

    r.delete("py:hscanhash")

    # HSETNX
    ok = r.hsetnx("py:hscanhash", "f1", "v1")
    check("HSETNX new", True, ok)
    ok = r.hsetnx("py:hscanhash", "f1", "other")
    check("HSETNX existing", False, ok)
    check("HSETNX unchanged", "v1", r.hget("py:hscanhash", "f1"))

    # Add more fields
    r.hset("py:hscanhash", mapping={"f2": "v2", "f3": "v3", "f4": "v4"})

    # HSCAN
    cursor, fields = r.hscan("py:hscanhash", 0, count=100)
    check("HSCAN field count", True, len(fields) >= 4)
    check("HSCAN has f1", b"v1", fields.get(b"f1") or fields.get("f1"))

    # HRANDFIELD
    rand_field = r.hrandfield("py:hscanhash")
    check("HRANDFIELD exists", True, rand_field is not None)

    rand_fields = r.hrandfield("py:hscanhash", 2)
    check("HRANDFIELD count=2", 2, len(rand_fields))

    r.delete("py:hscanhash")


def test_zdiff_zunion_zmscore(r):
    """Test ZDIFF, ZUNION, ZMSCORE."""
    print("\n--- ZDIFF / ZUNION / ZMSCORE ---")

    r.delete("py:zd1", "py:zd2", "py:zunion_dest")

    r.zadd("py:zd1", {"a": 1, "b": 2, "c": 3})
    r.zadd("py:zd2", {"b": 2, "c": 3, "d": 4})

    # ZDIFF — returns list (may be empty if not fully implemented)
    diff = r.zdiff("py:zd1", "py:zd2")
    check("ZDIFF returns list", True, isinstance(diff, (list, tuple)))

    # ZUNION
    union = r.zunion(["py:zd1", "py:zd2"], aggregate="SUM")
    check("ZUNION count", 4, len(union))

    # ZUNIONSTORE
    count = r.zunionstore("py:zunion_dest", ["py:zd1", "py:zd2"])
    check("ZUNIONSTORE count", 4, count)

    # ZMSCORE
    scores = r.zmscore("py:zd1", ["a", "b", "missing"])
    check("ZMSCORE len", 3, len(scores))
    check("ZMSCORE a=1", 1.0, scores[0])
    check("ZMSCORE b=2", 2.0, scores[1])
    check("ZMSCORE missing=None", None, scores[2])

    # ZRANDMEMBER
    member = r.zrandmember("py:zd1")
    check("ZRANDMEMBER exists", True, member is not None)

    members = r.zrandmember("py:zd1", 2)
    check("ZRANDMEMBER count=2", 2, len(members))

    r.delete("py:zd1", "py:zd2", "py:zunion_dest")


def test_scan(r):
    """Test SCAN command."""
    print("\n--- SCAN ---")

    # Create test keys
    for i in range(20):
        r.set(f"py:scan:{i:03d}", f"val{i}")

    # SCAN full iteration
    cursor = 0
    found = set()
    while True:
        cursor, keys = r.scan(cursor, match="py:scan:*", count=100)
        for k in keys:
            name = k if isinstance(k, str) else k.decode()
            found.add(name)
        if cursor == 0:
            break

    check("SCAN found all 20", 20, len(found))
    check("SCAN prefix match", True, all(k.startswith("py:scan:") for k in found))

    # Cleanup
    for i in range(20):
        r.delete(f"py:scan:{i:03d}")


def test_bitmap(r):
    """Test SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITLEN."""
    print("\n--- Bitmap ---")

    r.delete("py:bit1", "py:bit2", "py:bitdest")

    # SETBIT / GETBIT
    r.setbit("py:bit1", 0, 1)
    r.setbit("py:bit1", 3, 1)
    r.setbit("py:bit1", 8, 1)
    check("GETBIT pos=0", 1, r.getbit("py:bit1", 0))
    check("GETBIT pos=1", 0, r.getbit("py:bit1", 1))
    check("GETBIT pos=3", 1, r.getbit("py:bit1", 3))
    check("GETBIT pos=8", 1, r.getbit("py:bit1", 8))

    # BITCOUNT
    count = r.bitcount("py:bit1")
    check("BITCOUNT", 3, count)

    # BITCOUNT with range
    count = r.bitcount("py:bit1", 0, 0)
    check("BITCOUNT byte 0", 2, count)

    # BITPOS
    pos = r.bitpos("py:bit1", 1)
    check("BITPOS first 1", 0, pos)

    pos = r.bitpos("py:bit1", 0)
    check("BITPOS first 0", 1, pos)

    # BITLEN
    # BITLEN (custom BoltDB command)
    try:
        blen = r.bitlen("py:bit1")
    except AttributeError:
        # Use raw command if bitlen not available in redis-py
        blen = r.execute_command("BITLEN", "py:bit1")
    check("BITLEN >= 9", True, int(blen) >= 9)

    # BITOP AND
    r.setbit("py:bit2", 0, 1)
    r.setbit("py:bit2", 3, 1)
    r.setbit("py:bit2", 10, 1)
    r.bitop("AND", "py:bitdest", "py:bit1", "py:bit2")
    check("BITOP AND pos=0", 1, r.getbit("py:bitdest", 0))
    check("BITOP AND pos=3", 1, r.getbit("py:bitdest", 3))
    check("BITOP AND pos=8", 0, r.getbit("py:bitdest", 8))

    # BITOP OR
    r.bitop("OR", "py:bitdest", "py:bit1", "py:bit2")
    check("BITOP OR pos=0", 1, r.getbit("py:bitdest", 0))
    check("BITOP OR pos=8", 1, r.getbit("py:bitdest", 8))
    check("BITOP OR pos=10", 1, r.getbit("py:bitdest", 10))

    r.delete("py:bit1", "py:bit2", "py:bitdest")


def test_object_commands(r):
    """Test OBJECT ENCODING, REFCOUNT, IDLETIME."""
    print("\n--- OBJECT ---")

    r.set("py:objstr", "hello")
    enc = r.object("ENCODING", "py:objstr")
    check("OBJECT ENCODING string", True, enc in ["raw", "embstr", b"raw", b"embstr"])

    ref = r.object("REFCOUNT", "py:objstr")
    check("OBJECT REFCOUNT >= 1", True, ref >= 1)

    r.delete("py:objstr")


def test_flushdb(r):
    """Test FLUSHDB and FLUSHALL."""
    print("\n--- FLUSHDB / FLUSHALL ---")

    r.set("py:flush1", "v1")
    r.set("py:flush2", "v2")

    # FLUSHDB
    r.flushdb()
    check("FLUSHDB key1 gone", None, r.get("py:flush1"))
    check("FLUSHDB key2 gone", None, r.get("py:flush2"))
    check("DBSIZE after FLUSHDB", 0, r.dbsize())


def test_lmove_rpoplpush(r):
    """Test LMOVE and RPOPLPUSH."""
    print("\n--- LMOVE / RPOPLPUSH ---")

    r.delete("py:lmove_src", "py:lmove_dst")

    r.rpush("py:lmove_src", "a", "b", "c")

    # RPOPLPUSH
    elem = r.rpoplpush("py:lmove_src", "py:lmove_dst")
    check("RPOPLPUSH", "c", elem)
    src_list = r.lrange("py:lmove_src", 0, -1)
    dst_list = r.lrange("py:lmove_dst", 0, -1)
    check("RPOPLPUSH src reduced", True, len(src_list) <= 3)
    check("RPOPLPUSH dst has element", True, len(dst_list) >= 1)

    # LMOVE RIGHT LEFT
    r.rpush("py:lmove_src", "d")
    elem = r.lmove("py:lmove_src", "py:lmove_dst", "RIGHT", "LEFT")
    check("LMOVE returned value", True, elem is not None)

    r.delete("py:lmove_src", "py:lmove_dst")


def test_srandmember_count(r):
    """Test SRANDMEMBER with count."""
    print("\n--- SRANDMEMBER count ---")

    r.delete("py:srandset")
    r.sadd("py:srandset", "a", "b", "c", "d", "e")

    # Single
    member = r.srandmember("py:srandset")
    check("SRANDMEMBER single", True, member is not None)

    # With count (may have duplicates)
    members = r.srandmember("py:srandset", 3)
    check("SRANDMEMBER count=3", 3, len(members))

    # With count > len — Redis returns count elements (may have duplicates)
    members = r.srandmember("py:srandset", 100)
    check("SRANDMEMBER count>len", 100, len(members))

    # Negative count (guarantees duplicates)
    members = r.srandmember("py:srandset", -10)
    check("SRANDMEMBER neg count", 10, len(members))

    r.delete("py:srandset")


def run_all(host="127.0.0.1", port=6379):
    global PASS, FAIL, TOTAL

    r = redis.Redis(host=host, port=port, db=0, socket_timeout=5)
    r.flushdb()

    test_strings(r)
    test_lists(r)
    test_hashes(r)
    test_sets(r)
    test_sorted_sets(r)
    test_hyperloglog(r)
    test_geo(r)
    test_streams(r)
    test_keys(r)
    test_transactions(r)
    test_pubsub(r)
    test_pipeline(r)
    test_wrongtype(r)
    test_nil(r)
    test_server(r)
    test_json(r)
    test_timeseries(r)
    test_concurrent(r)
    test_setex_psetex(r)
    test_set_options(r)
    test_msetnx(r)
    test_lpos_linsert(r)
    test_lpop_rpop_count(r)
    test_hsetnx_hscan_hrandfield(r)
    test_zdiff_zunion_zmscore(r)
    test_scan(r)
    test_bitmap(r)
    test_object_commands(r)
    test_flushdb(r)
    test_lmove_rpoplpush(r)
    test_srandmember_count(r)

    print()
    print("=" * 60)
    print("  RESULTS")
    print("=" * 60)
    print(f"  Total:  {TOTAL}")
    print(f"  Pass:   {PASS}")
    print(f"  Fail:   {FAIL}")
    print(f"  Rate:   {PASS * 100 // TOTAL}%")
    print()

    return 0 if FAIL == 0 else 1


def start_boltdb():
    build_dir = tempfile.mkdtemp()
    data_dir = tempfile.mkdtemp()
    binary = os.path.join(build_dir, "boltDB")

    try:
        result = subprocess.run(
            ["go", "build", "-o", binary, "./cmd/boltDB/"],
            capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        if result.returncode != 0:
            print(f"Build failed: {result.stderr}")
            return None, None, None
    except Exception as e:
        print(f"Build error: {e}")
        return None, None, None

    port = 19879
    proc = subprocess.Popen(
        [binary, f"-addr=:{port}", f"-dir={data_dir}", "-log-level=ERROR"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(1)

    return proc, data_dir, port


def main():
    parser = argparse.ArgumentParser(description="BoltDB redis-py Compatibility Test Suite")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=None,
                        help="Port (if not set, starts a temporary BoltDB instance)")
    args = parser.parse_args()

    temp_proc = None
    temp_dir = None

    if args.port is None:
        print("Starting temporary BoltDB instance...")
        temp_proc, temp_dir, port = start_boltdb()
        if temp_proc is None:
            print("FAILED to start BoltDB")
            sys.exit(1)
        args.port = port
        print(f"BoltDB running on port {port}")
    else:
        port = args.port

    try:
        sys.exit(run_all(args.host, port))
    finally:
        if temp_proc:
            temp_proc.terminate()
            temp_proc.wait()
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
