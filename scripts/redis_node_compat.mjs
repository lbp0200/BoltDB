#!/usr/bin/env node
/**
 * BoltDB Redis Compatibility Test Suite (node-redis).
 *
 * Usage:
 *   node scripts/redis_node_compat.mjs [--port PORT]
 *
 *   Without --port, automatically builds and starts a temporary BoltDB instance.
 *   Requires: Node.js >= 18, npm package "redis" (npm install redis)
 */

import { createClient } from "redis";
import { execSync, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir, hostname } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..");

let PASS = 0;
let FAIL = 0;
let TOTAL = 0;

const RED = "\x1b[0;31m";
const GREEN = "\x1b[0;32m";
const NC = "\x1b[0m";

function check(name, expected, actual) {
  TOTAL++;
  const ok = typeof actual !== 'undefined' && actual !== null && (
    expected === actual ||
    (expected instanceof Buffer && actual instanceof Buffer && expected.equals(actual)) ||
    (Array.isArray(expected) && Array.isArray(actual) &&
      expected.length === actual.length &&
      expected.every((e, i) => actual[i] === e || (Buffer.isBuffer(actual[i]) && Buffer.isBuffer(e) && e.equals(actual[i]))))
  );
  if (ok) {
    console.log(`  ${GREEN}PASS${NC} ${name}`);
    PASS++;
  } else {
    console.log(`  ${RED}FAIL${NC} ${name}`);
    console.log(`    Expected: ${JSON.stringify(expected)}`);
    console.log(`    Actual:   ${JSON.stringify(actual)}`);
    FAIL++;
  }
}

function checkBulk(name, expectedStr, actual) {
  TOTAL++;
  const ok = actual !== null && String(actual) === expectedStr;
  if (ok) {
    console.log(`  ${GREEN}PASS${NC} ${name}`);
    PASS++;
  } else {
    console.log(`  ${RED}FAIL${NC} ${name}`);
    console.log(`    Expected: ${expectedStr}`);
    console.log(`    Actual:   ${actual !== null ? String(actual) : "null"}`);
    FAIL++;
  }
}

function checkNil(name, actual) {
  TOTAL++;
  if (actual === null) {
    console.log(`  ${GREEN}PASS${NC} ${name}`);
    PASS++;
  } else {
    console.log(`  ${RED}FAIL${NC} ${name} — expected null, got ${JSON.stringify(actual)}`);
    FAIL++;
  }
}

function checkErr(name, expectedSubstr, fn) {
  TOTAL++;
  fn().then(() => {
    console.log(`  ${RED}FAIL${NC} ${name} — expected error, got none`);
    FAIL++;
  }, (err) => {
    const msg = String(err.message || err);
    if (msg.includes(expectedSubstr)) {
      console.log(`  ${GREEN}PASS${NC} ${name}`);
      PASS++;
    } else {
      console.log(`  ${RED}FAIL${NC} ${name}`);
      console.log(`    Expected error containing: ${expectedSubstr}`);
      console.log(`    Got: ${msg}`);
      FAIL++;
    }
  });
}

function section(title) {
  console.log();
  console.log("=".repeat(60));
  console.log(title);
  console.log("=".repeat(60));
}

async function test_strings(r) {
  section("STRINGS");
  check("PING", "PONG", await r.ping());
  checkBulk("ECHO", "hello", await r.echo("hello"));

  await r.set("js:str", "value1");
  checkBulk("SET/GET", "value1", await r.get("js:str"));
  check("STRLEN", 6, await r.strLen("js:str"));
  check("APPEND", 15, await r.append("js:str", "_appended"));
  checkBulk("GET after APPEND", "value1_appended", await r.get("js:str"));
  await r.del("js:str");
  checkNil("GET nonexistent", await r.get("js:str"));

  await r.mSet({ "js:m1": "a", "js:m2": "b" });
  const mget = await r.mGet(["js:m1", "js:m2"]);
  check("MGET", 2, mget ? mget.length : 0);
  checkBulk("MGET[0]", "a", mget ? mget[0] : null);
  checkBulk("MGET[1]", "b", mget ? mget[1] : null);
  await r.del(["js:m1", "js:m2"]);

  await r.set("js:incr", "10");
  check("INCR", 11, await r.incr("js:incr"));
  check("INCRBY", 15, await r.incrBy("js:incr", 4));
  check("DECR", 14, await r.decr("js:incr"));
  check("DECRBY", 10, await r.decrBy("js:incr", 4));
  await r.del("js:incr");

  await r.set("js:float", "1.5");
  checkBulk("INCRBYFLOAT", "3", String(await r.incrByFloat("js:float", 1.5)));
  await r.del("js:float");

  check("SETNX new", true, Boolean(await r.setNX("js:setnx", "first")));
  check("SETNX existing", false, Boolean(await r.setNX("js:setnx", "second")));
  checkBulk("GETSET", "first", await r.getSet("js:setnx", "replaced"));
  checkBulk("GET after GETSET", "replaced", await r.get("js:setnx"));
  await r.del("js:setnx");

  await r.set("js:range", "abcdefgh");
  check("GETRANGE", "cde", await r.getRange("js:range", 2, 4));
  check("SETRANGE", 8, await r.setRange("js:range", 2, "XYZ"));
  checkBulk("GET after SETRANGE", "abXYZfgh", await r.get("js:range"));
  await r.del("js:range");
}

async function test_lists(r) {
  section("LISTS");
  check("LPUSH", 2, await r.lPush("js:list", ["b", "a"]));
  check("RPUSH", 4, await r.rPush("js:list", ["c", "d"]));
  check("LLEN", 4, await r.lLen("js:list"));
  check("LRANGE", ["a", "b", "c", "d"], await r.lRange("js:list", 0, -1));
  checkBulk("LPOP", "a", await r.lPop("js:list"));
  checkBulk("RPOP", "d", await r.rPop("js:list"));
  check("LLEN after pop", 2, await r.lLen("js:list"));
  checkBulk("LINDEX", "c", await r.lIndex("js:list", 1));
  checkNil("LINDEX out of range", await r.lIndex("js:list", 999));
  await r.lSet("js:list", 0, "modified");
  checkBulk("LSET", "modified", await r.lIndex("js:list", 0));
  await r.lPush("js:list", ["x", "x", "x"]);
  check("LREM", 3, await r.lRem("js:list", 3, "x"));
  check("LRANGE after LREM", ["modified", "c"], await r.lRange("js:list", 0, -1));
  await r.rPush("js:trim", ["a", "b", "c", "d"]);
  await r.lTrim("js:trim", 1, 2);
  check("LTRIM", ["b", "c"], await r.lRange("js:trim", 0, -1));
  await r.del("js:trim");
  await r.rPush("js:len", ["a", "b", "c"]);
  check("LLEN non-empty", 3, await r.lLen("js:len"));
  await r.del("js:len");
  await r.rPush("js:rpoplpush", ["a", "b", "c"]);
  checkBulk("RPOPLPUSH", "c", await r.rPopLPush("js:rpoplpush", "js:rpoplpush_dest"));
  check("LRANGE rpoplpush src", ["a", "b"], await r.lRange("js:rpoplpush", 0, -1));
  await r.del(["js:rpoplpush", "js:rpoplpush_dest"]);

  checkNil("BLPOP timeout", await r.blPop(["js:bl_empty"], 1));
  checkNil("BRPOP timeout", await r.brPop(["js:br_empty"], 1));
  await r.rPush("js:bldata", "val1", "val2");
  const blResult = await r.blPop(["js:bldata"], 1);
  check("BLPOP with data", "js:bldata", blResult ? blResult.key : null);
  checkBulk("BLPOP value", "val1", blResult ? blResult.element : null);
  await r.del("js:bldata");
}

async function test_hashes(r) {
  section("HASHES");
  await r.hSet("js:hash", "field1", "val1");
  checkBulk("HGET", "val1", await r.hGet("js:hash", "field1"));
  check("HEXISTS", true, Boolean(await r.hExists("js:hash", "field1")));
  check("HEXISTS missing", false, Boolean(await r.hExists("js:hash", "nope")));
  check("HLEN", 1, await r.hLen("js:hash"));
  check("HDEL", 1, await r.hDel("js:hash", "field1"));
  check("HLEN after DEL", 0, await r.hLen("js:hash"));
  await r.del("js:hash");

  await r.hSet("js:hmulti", { f1: "a", f2: "b" });
  check("HGETALL", 2, Object.keys(await r.hGetAll("js:hmulti")).length);
  checkBulk("HGETALL f1", "a", (await r.hGetAll("js:hmulti"))["f1"]);
  checkBulk("HGETALL f2", "b", (await r.hGetAll("js:hmulti"))["f2"]);
  await r.hSet("js:hkeys", { k1: "v1", k2: "v2", k3: "v3" });
  check("HKEYS", 3, (await r.hKeys("js:hkeys")).length);
  check("HVALS", 3, (await r.hVals("js:hkeys")).length);
  await r.del("js:hkeys");

  check("HINCRBY", 5, await r.hIncrBy("js:hicr", "counter", 5));
  check("HINCRBY again", 8, await r.hIncrBy("js:hicr", "counter", 3));
  check("HSTRLEN", 1, await r.hStrLen("js:hicr", "counter"));
  await r.del("js:hicr");

  check("HSETNX new", true, Boolean(await r.hSetNX("js:hnx", "f1", "x")));
  check("HSETNX existing", false, Boolean(await r.hSetNX("js:hnx", "f1", "y")));
  await r.del("js:hnx");
}

async function test_sets(r) {
  section("SETS");
  check("SADD", 2, await r.sAdd("js:set", ["a", "b"]));
  check("SCARD", 2, await r.sCard("js:set"));
  check("SISMEMBER", true, Boolean(await r.sIsMember("js:set", "a")));
  check("SISMEMBER missing", false, Boolean(await r.sIsMember("js:set", "z")));
  check("SMEMBERS", 2, (await r.sMembers("js:set")).length);
  const spopResult = await r.sPop("js:set", 1);
  check("SPOP", 1, spopResult.length);
  const sremResult = await r.sRem("js:set", spopResult[0]);
  check("SREM after SPOP", 0, sremResult);
  check("SCARD after SREM", 1, await r.sCard("js:set"));
  await r.del("js:set");

  await r.sAdd("js:sa", ["a", "b", "c"]);
  await r.sAdd("js:sb", ["b", "c", "d"]);
  check("SINTER", ["b", "c"], (await r.sInter(["js:sa", "js:sb"])).sort());
  check("SUNION", 4, (await r.sUnion(["js:sa", "js:sb"])).length);
  check("SDIFF", ["a"], await r.sDiff(["js:sa", "js:sb"]));
  await r.del(["js:sa", "js:sb"]);
}

async function test_zsets(r) {
  section("SORTED SETS");
  check("ZADD", 2, await r.zAdd("js:z", [{ score: 1, value: "a" }, { score: 2, value: "b" }]));
  check("ZCARD", 2, await r.zCard("js:z"));
  check("ZRANGE", ["a", "b"], await r.zRange("js:z", 0, -1));
  check("ZSCORE", 2, await r.zScore("js:z", "b"));
  check("ZRANK", 0, await r.zRank("js:z", "a"));
  check("ZREM", 1, await r.zRem("js:z", "a"));
  check("ZCARD after ZREM", 1, await r.zCard("js:z"));

  await r.zAdd("js:z2", [{ score: 1, value: "x" }, { score: 5, value: "y" }, { score: 10, value: "z" }]);
  check("ZCOUNT", 2, await r.zCount("js:z2", 1, 6));
  check("ZRANGEBYSCORE", ["x", "y"], await r.zRangeByScore("js:z2", 1, 6));
  check("ZRANK b", 1, await r.zRank("js:z2", "y"));
  check("ZINCRBY", 6, await r.zIncrBy("js:z2", 1, "y"));
  await r.del(["js:z", "js:z2"]);

  await r.zAdd("js:za", [{ score: 1, value: "a" }, { score: 2, value: "b" }]);
  await r.zAdd("js:zb", [{ score: 1, value: "b" }, { score: 2, value: "c" }]);
  check("ZINTERSTORE", 1, await r.zInterStore("js:zout", ["js:za", "js:zb"]));
  await r.del(["js:za", "js:zb", "js:zout"]);
}

async function test_geo(r) {
  section("GEO");
  await r.geoAdd("js:geo", { longitude: 13.361389, latitude: 38.115556, member: "Palermo" });
  await r.geoAdd("js:geo", { longitude: 15.087269, latitude: 37.502669, member: "Catania" });
  check("GEOADD ok", true, await r.geoAdd("js:geo2", { longitude: 0, latitude: 0, member: "origin" }) > 0);
  const dist = parseFloat(await r.geoDist("js:geo", "Palermo", "Catania", "m"));
  const distOk = Math.abs(dist - 166274.1516) < 200;
  if (distOk) { PASS++; } else { FAIL++; }
  TOTAL++;
  console.log(`  ${distOk ? GREEN + "PASS" : RED + "FAIL"}${NC} GEODIST (got ${dist.toFixed(4)}, expected ~166274.1516)`);
  const geoPosResults = await r.geoPos("js:geo", ["Palermo", "Catania"]);
  check("GEOPOS", 2, geoPosResults.length);
  try {
    const georadius = await r.sendCommand(["GEORADIUS", "js:geo", "15", "37", "200", "km"]);
    check("GEORADIUS", 2, georadius.length);
  } catch (e) {
    console.log(`  ${RED}FAIL${NC} GEORADIUS — ${e.message}`);
    FAIL++; TOTAL++;
  }
  await r.del(["js:geo", "js:geo2"]);
}

async function test_streams(r) {
  section("STREAMS");
  try {
    const sid = await r.xAdd("js:stream", "*", { foo: "bar", num: "42" });
    check("XADD ok", true, typeof sid === "string" && sid.length > 0);
  } catch (e) {
    console.log(`  ${RED}FAIL${NC} XADD — ${e.message}`);
    FAIL++; TOTAL++;
  }
  try {
    const len = await r.xLen("js:stream");
    check("XLEN", 1, len);
  } catch (e) {
    console.log(`  ${RED}FAIL${NC} XLEN — ${e.message}`);
    FAIL++; TOTAL++;
  }
  try {
    const range = await r.xRange("js:stream", "-", "+");
    check("XRANGE", 1, range.length);
  } catch (e) {
    console.log(`  ${RED}FAIL${NC} XRANGE — ${e.message}`);
    FAIL++; TOTAL++;
  }
  await r.del("js:stream");
}

async function test_pubsub(r) {
  section("PUBSUB");
  const sub = createClient({ url: r.options?.url || "redis://127.0.0.1:16379" });
  await sub.connect();
  const messages = [];
  await sub.subscribe("js:ch", (msg) => messages.push(msg));
  await new Promise(r => setTimeout(r, 100));
  check("PUBLISH", 1, await r.publish("js:ch", "hello"));
  await new Promise(r => setTimeout(r, 200));
  check("SUBSCRIBE message", 1, messages.length);
  checkBulk("SUBSCRIBE content", "hello", messages[0]);
  await sub.unsubscribe("js:ch");
  await sub.quit();
}

async function test_keys(r) {
  section("KEYS");
  await r.set("js:k_a", "1");
  await r.set("js:k_b", "2");
  await r.set("js:k_c", "3");
  check("EXISTS", 3, await r.exists(["js:k_a", "js:k_b", "js:k_c"]));
  check("EXISTS missing", 0, await r.exists(["js:nonexistent"]));
  check("TYPE string", "string", await r.type("js:k_a"));
  check("DEL", 2, await r.del(["js:k_a", "js:k_b"]));
  check("EXISTS after DEL", 1, await r.exists(["js:k_c"]));
  await r.expire("js:k_c", 10);
  check("TTL", true, (await r.ttl("js:k_c")) > 0);
  await r.persist("js:k_c");
  check("TTL after PERSIST", -1, await r.ttl("js:k_c"));
  await r.del("js:k_c");
}

async function test_transactions(r) {
  section("TRANSACTIONS");
  const multi = r.multi();
  multi.set("js:tx_a", "tx_val");
  multi.get("js:tx_a");
  multi.incr("js:tx_counter");
  const results = await multi.exec();
  check("MULTI EXEC count", 3, results.length);
  check("MULTI SET", "OK", results[0]);
  check("MULTI GET", "tx_val", results[1]);
  check("MULTI INCR", 1, results[2]);
  await r.del(["js:tx_a", "js:tx_counter"]);
}

async function test_wrongtype(r) {
  section("WRONGTYPE");
  await r.set("js:wt", "string_val");
  checkErr("LLEN on string", "WRONGTYPE", () => r.lLen("js:wt"));
  checkErr("LPOP on string", "WRONGTYPE", () => r.lPop("js:wt"));
  checkErr("SADD on string", "WRONGTYPE", () => r.sAdd("js:wt", "m"));
  checkErr("ZADD on string", "WRONGTYPE", () => r.zAdd("js:wt", [{ score: 1, value: "x" }]));
  checkErr("HSET on string", "WRONGTYPE", () => r.hSet("js:wt", "f", "v"));
  checkErr("XADD on string", "WRONGTYPE", () => r.xAdd("js:wt", "*", { f: "v" }));
  await r.del("js:wt");
}

async function test_server(r) {
  section("SERVER");
  const info = await r.info();
  check("INFO", true, info && info.length > 0);
  const dbsize = await r.dbSize();
  check("DBSIZE", true, typeof dbsize === "number");
}

async function run_all(port) {
  const r = createClient({ url: `redis://127.0.0.1:${port}` });
  await r.connect();

  try {
    await test_strings(r);
    await test_lists(r);
    await test_hashes(r);
    await test_sets(r);
    await test_zsets(r);
    await test_geo(r);
    await test_streams(r);
    await test_pubsub(r);
    await test_keys(r);
    await test_transactions(r);
    await test_wrongtype(r);
    await test_server(r);
  } finally {
    try { await r.quit(); } catch (e) { /* ignore quit errors */ }
  }

  console.log();
  console.log("=".repeat(60));
  console.log("  RESULTS");
  console.log("=".repeat(60));
  console.log(`  Total:  ${TOTAL}`);
  console.log(`  Pass:   ${PASS}`);
  console.log(`  Fail:   ${FAIL}`);
  console.log(`  Rate:   ${Math.floor(PASS * 100 / TOTAL)}%`);
  console.log();

  return FAIL === 0 ? 0 : 1;
}

function isPortAvailable(port) {
  try {
    execSync(`lsof -i :${port}`, { stdio: "pipe" });
    return false;
  } catch {
    return true;
  }
}

async function startBoltdb() {
  const dataDir = mkdtempSync(join(tmpdir(), "boltdb_node_"));
  let port = 19889;
  while (!isPortAvailable(port) && port < 19999) {
    port++;
  }

  const binary = join(dataDir, "boltDB");
  execSync(`go build -a -o ${binary} cmd/boltDB/main.go`, {
    cwd: REPO_ROOT,
    stdio: "pipe",
  });

  const proc = spawn(binary, [`-addr=:${port}`, `-dir=${dataDir}`, "-log-level=ERROR"], {
    stdio: "ignore",
  });

  return { proc, dataDir, port };
}

async function main() {
  const args = process.argv.slice(2);
  let port = null;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--port" && i + 1 < args.length) {
      port = parseInt(args[i + 1], 10);
    }
  }

  let tempProc = null;
  let tempDir = null;

  if (port === null) {
    console.log("Starting temporary BoltDB instance...");
    try {
      const result = await startBoltdb();
      tempProc = result.proc;
      tempDir = result.dataDir;
      port = result.port;
    } catch (e) {
      console.error("FAILED to start BoltDB:", e.message);
      process.exit(1);
    }
    console.log(`BoltDB running on port ${port}`);
    await new Promise((r) => setTimeout(r, 1500));
  }

  try {
    const code = await run_all(port);
    process.exit(code);
  } finally {
    if (tempProc) {
      tempProc.kill();
      try { tempProc.kill("SIGKILL"); } catch {}
    }
    if (tempDir) {
      rmSync(tempDir, { recursive: true, force: true });
    }
  }
}

try {
  await main();
} catch (e) {
  console.error("FATAL:", e.message);
  process.exit(1);
}
