/**
 * MySQL 驱动（Cloudflare Hyperdrive / Node.js 直连双模式）
 *
 * 两种运行方式共用同一份实现与 MySQL 方言：
 *
 * 1. Cloudflare Workers + Hyperdrive（推荐）
 *    - 绑定名固定为 `HYPERDRIVE`（与 Cloudflare 官方文档一致）。
 *    - 从 `env.HYPERDRIVE` 读取 host/user/password/database/port（或
 *      connectionString），通过 mysql2/promise 建连。
 *    - Hyperdrive 自身维护底层连接池，因此这里**按操作创建连接并在结束时
 *      关闭**，不复用模块级 Pool；否则 Workers isolate 内长期持有 socket
 *      会绕过 Hyperdrive 的连接管理。
 *
 * 2. Node.js 容器直连（兼容旧部署）
 *    - MYSQL_URLS（优先，连接串）
 *    - MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_NAME
 *    - 保持原有模块级 Pool 复用逻辑。
 *
 * 驱动名仍为 `mysql`：SQL 格式适配器与表结构按该名字选择 MySQL 方言，
 * 因此 `DB_FORMAT=map|key|sql` 在两种模式下都可用。
 */
import type { Driver } from "../types"
import { buildDdl, getTablePrefix, KV_SCHEMA_MYSQL } from "../schema"

type MysqlConfig = Record<string, any>
type QueryFn = (sql: string, params: any[]) => Promise<any[]>

interface TxHandle {
  begin(): Promise<void>
  commit(): Promise<void>
  rollback(): Promise<void>
}

interface ClientSession {
  /** 当前会话的 schema 去重键（含连接来源与 database） */
  schemaKey: string
  query: QueryFn
  tx?: TxHandle
  close(): Promise<void>
}

/** 测试注入点：避免依赖真实 mysql2 / Hyperdrive 网络。 */
let mysqlModuleOverride: any = null

export function __setMysqlModuleForTest(mod: any | null): void {
  mysqlModuleOverride = mod
}

export function __resetMysqlDriverStateForTest(): void {
  mysqlModuleOverride = null
  _pool = null
  _poolKey = null
  _schemaInitedKeys.clear()
}

async function loadMysql2(): Promise<any> {
  if (mysqlModuleOverride) return mysqlModuleOverride
  // 动态 import：EdgeOne / ESA 构建会把 mysql2 替换为空桩；只有真正走到
  // MySQL 驱动时才会加载。Hyperdrive 场景下 Wrangler 会将其打进 Worker。
  return await import("mysql2/promise")
}

function isNode(): boolean {
  return typeof process !== "undefined" && process.release?.name === "node"
}

/** 解析 mysql://user:pass@host:3306/db 形态的连接串。 */
function parseMysqlUrl(url: string): MysqlConfig | null {
  try {
    const u = new URL(url)
    if (u.protocol !== "mysql:" && u.protocol !== "mariadb:") return null
    const config: MysqlConfig = {
      host: decodeURIComponent(u.hostname),
      port: Number(u.port || 3306),
      user: decodeURIComponent(u.username),
      password: decodeURIComponent(u.password),
      database: decodeURIComponent(u.pathname.replace(/^\//, "")),
    }
    // 常见连接串参数：ssl=true / sslmode=REQUIRED（mysql2 用 ssl:{} 开启 TLS）
    const ssl = u.searchParams.get("ssl") || u.searchParams.get("sslmode")
    if (ssl && ssl.toLowerCase() !== "false" && ssl.toLowerCase() !== "disabled") {
      config.ssl = {}
    }
    return config
  } catch {
    return null
  }
}

/**
 * 读取 Hyperdrive binding 配置。
 *
 * Cloudflare 官方 binding 同时暴露分项字段与 connectionString；优先用分项
 * 字段（与官方 mysql2 示例一致），缺失时回退解析 connectionString，以兼容
 * 本地 `localConnectionString` 与只注入 connectionString 的运行时。
 */
export function readHyperdriveConfig(env?: any): MysqlConfig | null {
  const h = env?.HYPERDRIVE
  if (!h) return null

  let config: MysqlConfig | null = null
  if (typeof h === "string") {
    config = parseMysqlUrl(h)
  } else if (h.host) {
    config = {
      host: String(h.host),
      port: Number(h.port || 3306),
      user: String(h.user ?? ""),
      password: String(h.password ?? ""),
      database: String(h.database ?? ""),
    }
    if (h.ssl) config.ssl = h.ssl
  } else if (h.connectionString) {
    config = parseMysqlUrl(String(h.connectionString))
  }

  if (!config || !config.host) return null
  // Workers 不支持 eval-based SQL parser（官方示例要求）
  config.disableEval = true
  return config
}

export function hasHyperdriveBinding(env?: any): boolean {
  return readHyperdriveConfig(env) != null
}

/** Node 直连模式配置（保持旧行为）。 */
function getNodeConfig(env?: any): any | null {
  const e = env || (typeof process !== "undefined" ? process.env : {}) || {}
  const url = e?.MYSQL_URLS
  if (url) return url

  const host = e?.MYSQL_HOST
  if (!host) return null
  return {
    host,
    port: Number(e?.MYSQL_PORT || 3306),
    user: e?.MYSQL_USER || "",
    password: e?.MYSQL_PASS || "",
    database: e?.MYSQL_NAME || "",
  }
}

let _pool: any = null
let _poolKey: string | null = null

async function getNodePool(env?: any): Promise<any | null> {
  const config = getNodeConfig(env)
  if (!config) return null
  const key = JSON.stringify(config)
  if (_pool && _poolKey === key) return _pool

  const { createPool } = await loadMysql2()
  _pool = createPool(config)
  _poolKey = key
  return _pool
}

/** schema 去重键：不同 database / 不同连接方式不能共用同一份标记。 */
const _schemaInitedKeys = new Set<string>()

async function ensureSchema(session: ClientSession, env?: any): Promise<void> {
  const key = `${session.schemaKey}:${getTablePrefix(env)}`
  if (_schemaInitedKeys.has(key)) return

  // KV 表（map/key 格式）+ 列式表（sql 格式）一并创建
  for (const ddl of [...KV_SCHEMA_MYSQL, ...buildDdl("mysql", env)]) {
    await session.query(ddl, [])
  }
  _schemaInitedKeys.add(key)
}

/** 创建一次会话：Hyperdrive 用独立连接，Node 直连复用 Pool。 */
async function createSession(
  env: any,
  transaction: boolean,
): Promise<ClientSession> {
  const hd = readHyperdriveConfig(env)

  if (hd) {
    const { createConnection } = await loadMysql2()
    const conn = await createConnection(hd)
    const query: QueryFn = async (sql, params) => {
      const [rows]: any[] = await conn.query(sql, params)
      return rows || []
    }
    return {
      schemaKey: `hyperdrive:${hd.database || ""}`,
      query,
      tx: {
        begin: () => conn.beginTransaction(),
        commit: () => conn.commit(),
        rollback: () => conn.rollback(),
      },
      close: async () => {
        try {
          await conn.end()
        } catch {
          // 连接已断开时无需处理
        }
      },
    }
  }

  const pool = await getNodePool(env)
  if (!pool) throw new Error("MySQL pool not available")

  const key = `node:${String(getNodeConfig(env))}`
  if (transaction) {
    const conn = await pool.getConnection()
    const query: QueryFn = async (sql, params) => {
      const [rows]: any[] = await conn.query(sql, params)
      return rows || []
    }
    return {
      schemaKey: key,
      query,
      tx: {
        begin: () => conn.beginTransaction(),
        commit: () => conn.commit(),
        rollback: () => conn.rollback(),
      },
      close: async () => conn.release(),
    }
  }

  const query: QueryFn = async (sql, params) => {
    const [rows]: any[] = await pool.query(sql, params)
    return rows || []
  }
  return {
    schemaKey: key,
    query,
    close: async () => {},
  }
}

async function withClient<T>(
  env: any,
  fn: (session: ClientSession) => Promise<T>,
  opts: { transaction?: boolean } = {},
): Promise<T> {
  const session = await createSession(env, Boolean(opts.transaction))
  try {
    return await fn(session)
  } finally {
    await session.close()
  }
}

export const mysqlDriver: Driver = {
  name: "mysql",

  async isAvailable(env?: any): Promise<boolean> {
    // Hyperdrive 是 binding，不要求 Node 运行时
    if (hasHyperdriveBinding(env)) return true
    if (!isNode()) return false
    return getNodeConfig(env) != null
  },

  async init(env?: any): Promise<void> {
    if (!hasHyperdriveBinding(env) && !getNodeConfig(env)) return
    await withClient(env, async (session) => {
      await ensureSchema(session, env)
    })
  },

  async get(key: string, env?: any): Promise<string | null> {
    return await withClient(env, async (session) => {
      await ensureSchema(session, env)
      const rows = await session.query(
        "SELECT `value` FROM `kv` WHERE `key` = ?",
        [key],
      )
      return rows?.[0]?.value || null
    })
  },

  async put(key: string, value: string, env?: any): Promise<void> {
    await withClient(env, async (session) => {
      await ensureSchema(session, env)
      await session.query(
        "INSERT INTO `kv` (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)",
        [key, value],
      )
    })
  },

  async delete(key: string, env?: any): Promise<void> {
    await withClient(env, async (session) => {
      await ensureSchema(session, env)
      await session.query("DELETE FROM `kv` WHERE `key` = ?", [key])
    })
  },

  async list(prefix: string, env?: any): Promise<string[]> {
    return await withClient(env, async (session) => {
      await ensureSchema(session, env)
      const rows = await session.query(
        "SELECT `key` FROM `kv` WHERE `key` LIKE ? ORDER BY `key`",
        [`${prefix}%`],
      )
      return (rows || []).map((r: any) => r.key)
    })
  },

  async query(sql: string, params: any[], env?: any): Promise<any[]> {
    return await withClient(env, async (session) => {
      await ensureSchema(session, env)
      return await session.query(sql, params)
    })
  },

  async execute(sql: string, params: any[], env?: any): Promise<void> {
    await withClient(env, async (session) => {
      await ensureSchema(session, env)
      await session.query(sql, params)
    })
  },

  async batch(
    statements: Array<{ sql: string; params: any[] }>,
    env?: any,
  ): Promise<void> {
    await withClient(
      env,
      async (session) => {
        await ensureSchema(session, env)
        if (!session.tx) throw new Error("MySQL transaction is not available")
        await session.tx.begin()
        try {
          for (const stmt of statements) {
            await session.query(stmt.sql, stmt.params)
          }
          await session.tx.commit()
        } catch (err) {
          try {
            await session.tx.rollback()
          } catch {}
          throw err
        }
      },
      { transaction: true },
    )
  },

  async health(env?: any): Promise<any> {
    const hd = readHyperdriveConfig(env)
    if (hd) {
      try {
        await withClient(env, async (session) => {
          await session.query("SELECT 1", [])
        })
        return {
          configured: true,
          connected: true,
          platform: "MySQL (Hyperdrive)",
          mode: "hyperdrive",
        }
      } catch (err: any) {
        return {
          configured: true,
          connected: false,
          platform: "MySQL (Hyperdrive)",
          mode: "hyperdrive",
          error: err?.message || String(err),
        }
      }
    }

    if (!isNode()) {
      return {
        configured: false,
        connected: false,
        platform: "MySQL (mysql2)",
        mode: "mysql",
        error: "MySQL backend requires Hyperdrive binding or Node.js runtime",
      }
    }

    const config = getNodeConfig(env)
    if (!config) {
      return {
        configured: false,
        connected: false,
        platform: "MySQL (mysql2)",
        mode: "mysql",
        error: "MySQL config not found (expected MYSQL_URLS or MYSQL_HOST)",
      }
    }

    try {
      await withClient(env, async (session) => {
        await session.query("SELECT 1", [])
      })
      return {
        configured: true,
        connected: true,
        platform: "MySQL (mysql2)",
        mode: "mysql",
      }
    } catch (err: any) {
      return {
        configured: true,
        connected: false,
        platform: "MySQL (mysql2)",
        mode: "mysql",
        error: err?.message || String(err),
      }
    }
  },
}
