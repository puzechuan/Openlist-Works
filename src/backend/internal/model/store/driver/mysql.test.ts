import assert from "node:assert/strict"
import { afterEach, beforeEach, test } from "node:test"

import {
  __resetMysqlDriverStateForTest,
  __setMysqlModuleForTest,
  hasHyperdriveBinding,
  mysqlDriver,
  readHyperdriveConfig,
} from "./mysql"
import { getStorageBackend } from "../backend"

type Call = { type: string; sql?: string; params?: any[] }

function hyperdriveEnv(extra: Record<string, any> = {}) {
  return {
    HYPERDRIVE: {
      host: "hyperdrive.internal",
      port: 3306,
      user: "openlist",
      password: "secret",
      database: "openlist_db",
    },
    ...extra,
  }
}

function makeConn(options: { failSql?: string; kv?: Map<string, string> } = {}) {
  const calls: Call[] = []
  const kv = options.kv ?? new Map<string, string>()
  const conn: any = {
    query: async (sql: string, params: any[] = []) => {
      calls.push({ type: "query", sql, params })
      if (sql === options.failSql) throw new Error("query failed")
      if (sql.startsWith("SELECT `value`")) {
        return [[{ value: kv.get(String(params[0])) ?? null }]]
      }
      if (sql.startsWith("SELECT `key`")) {
        const prefix = String(params[0] ?? "").slice(0, -1)
        return [[...kv.keys()].filter((k) => k.startsWith(prefix)).map((key) => ({ key }))]
      }
      if (sql.startsWith("INSERT INTO `kv`")) {
        kv.set(String(params[0]), String(params[1]))
      }
      if (sql.startsWith("DELETE FROM `kv`")) {
        kv.delete(String(params[0]))
      }
      return [[]]
    },
    beginTransaction: async () => void calls.push({ type: "begin" }),
    commit: async () => void calls.push({ type: "commit" }),
    rollback: async () => void calls.push({ type: "rollback" }),
    end: async () => void calls.push({ type: "end" }),
  }
  return { conn, calls, kv }
}

beforeEach(() => __resetMysqlDriverStateForTest())
afterEach(() => {
  __setMysqlModuleForTest(null)
  __resetMysqlDriverStateForTest()
})

test("readHyperdriveConfig parses binding fields and connectionString", () => {
  const fromFields = readHyperdriveConfig(hyperdriveEnv())
  assert.equal(fromFields?.host, "hyperdrive.internal")
  assert.equal(fromFields?.port, 3306)
  assert.equal(fromFields?.user, "openlist")
  assert.equal(fromFields?.password, "secret")
  assert.equal(fromFields?.database, "openlist_db")
  assert.equal(fromFields?.disableEval, true)
  assert.equal(hasHyperdriveBinding(hyperdriveEnv()), true)

  const fromUrl = readHyperdriveConfig({
    HYPERDRIVE: {
      connectionString: "mysql://u:p%40ss@db.example.com:3307/openlist?sslmode=REQUIRED",
    },
  })
  assert.equal(fromUrl?.host, "db.example.com")
  assert.equal(fromUrl?.port, 3307)
  assert.equal(fromUrl?.user, "u")
  assert.equal(fromUrl?.password, "p@ss")
  assert.equal(fromUrl?.database, "openlist")
  assert.deepEqual(fromUrl?.ssl, {})
  assert.equal(fromUrl?.disableEval, true)

  assert.equal(
    readHyperdriveConfig({ HYPERDRIVE: { connectionString: "postgres://x/y" } }),
    null,
  )
  assert.equal(readHyperdriveConfig({}), null)
})

test("KV operations go through Hyperdrive with a fresh connection per operation", async () => {
  const { conn, calls } = makeConn()
  let createConnectionCalls = 0
  __setMysqlModuleForTest({
    createConnection: async (config: any) => {
      createConnectionCalls++
      assert.equal(config.host, "hyperdrive.internal")
      assert.equal(config.user, "openlist")
      assert.equal(config.password, "secret")
      assert.equal(config.database, "openlist_db")
      assert.equal(config.disableEval, true)
      return conn
    },
    createPool: () => {
      throw new Error("Hyperdrive mode must not use createPool")
    },
  })

  const env = hyperdriveEnv()
  assert.equal(await mysqlDriver.isAvailable(env), true)

  await mysqlDriver.put("a", "1", env)
  assert.equal(await mysqlDriver.get("a", env), "1")
  assert.deepEqual(await mysqlDriver.list("a", env), ["a"])
  await mysqlDriver.delete("a", env)
  assert.equal(await mysqlDriver.get("a", env), null)

  assert.equal(createConnectionCalls, 5)
  assert.equal(calls.filter((c) => c.type === "end").length, 5)
  assert.ok(calls.some((c) => c.sql?.startsWith("CREATE TABLE IF NOT EXISTS `kv`")))
})

test("batch uses one connection, commits on success and rolls back on failure", async () => {
  const success = makeConn()
  let successConnects = 0
  __setMysqlModuleForTest({
    createConnection: async () => {
      successConnects++
      return success.conn
    },
  })
  const env = hyperdriveEnv()
  await mysqlDriver.batch!(
    [
      { sql: "INSERT INTO `x_setting_items` (`key`) VALUES (?)", params: ["k"] },
      { sql: "UPDATE `x_setting_items` SET `value` = ?", params: ["v"] },
    ],
    env,
  )
  assert.equal(successConnects, 1)
  assert.equal(success.calls.filter((c) => c.type === "begin").length, 1)
  assert.equal(success.calls.filter((c) => c.type === "commit").length, 1)
  assert.equal(success.calls.filter((c) => c.type === "rollback").length, 0)
  assert.equal(success.calls.filter((c) => c.type === "end").length, 1)

  __resetMysqlDriverStateForTest()
  const failure = makeConn({ failSql: "BOOM" })
  __setMysqlModuleForTest({
    createConnection: async () => failure.conn,
  })
  await assert.rejects(
    mysqlDriver.batch!(
      [
        { sql: "INSERT INTO `x_setting_items` (`key`) VALUES (?)", params: ["k"] },
        { sql: "BOOM", params: [] },
      ],
      env,
    ),
    /query failed/,
  )
  assert.equal(failure.calls.filter((c) => c.type === "begin").length, 1)
  assert.equal(failure.calls.filter((c) => c.type === "commit").length, 0)
  assert.equal(failure.calls.filter((c) => c.type === "rollback").length, 1)
  assert.equal(failure.calls.filter((c) => c.type === "end").length, 1)
})

test("auto detection selects the mysql driver when Hyperdrive is bound", async () => {
  const { conn, calls } = makeConn()
  __setMysqlModuleForTest({
    createConnection: async () => conn,
  })
  const env = hyperdriveEnv({ DB_DRIVER: "auto", DB_FORMAT: "map" })
  const { driver, format } = await getStorageBackend(env)
  assert.equal(driver.name, "mysql")
  assert.equal(format.name, "map")
  assert.ok(calls.some((c) => c.sql?.startsWith("CREATE TABLE")))
})

test("Node direct mode still reuses a mysql2 pool", async () => {
  const { conn, calls } = makeConn()
  let createPoolCalls = 0
  __setMysqlModuleForTest({
    createPool: (config: any) => {
      createPoolCalls++
      assert.equal(config.host, "127.0.0.1")
      return {
        query: async (sql: string, params: any[]) => {
          calls.push({ type: "query", sql, params })
          return [[]]
        },
        getConnection: async () => conn,
      }
    },
    createConnection: () => {
      throw new Error("Node mode must not use createConnection")
    },
  })

  const env = {
    DB_DRIVER: "mysql",
    DB_FORMAT: "map",
    MYSQL_HOST: "127.0.0.1",
    MYSQL_PORT: "3306",
    MYSQL_USER: "root",
    MYSQL_PASS: "",
    MYSQL_NAME: "openlist",
  }
  assert.equal(await mysqlDriver.isAvailable(env), true)
  await mysqlDriver.query!("SELECT 1", [], env)
  await mysqlDriver.query!("SELECT 1", [], env)
  assert.equal(createPoolCalls, 1)
})

test("health reports Hyperdrive mode", async () => {
  const { conn } = makeConn()
  __setMysqlModuleForTest({
    createConnection: async () => conn,
  })
  const status = await mysqlDriver.health(hyperdriveEnv())
  assert.equal(status.configured, true)
  assert.equal(status.connected, true)
  assert.equal(status.mode, "hyperdrive")
  assert.equal(status.platform, "MySQL (Hyperdrive)")
})
