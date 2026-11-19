/**
 * 历史遗留文件：早期 wrangler.jsonc 曾把 `iconv-lite` alias 到本桩，以绕过 FTP
 * 驱动解析 safer-buffer 的打包问题（issue #151）。但 mysql2（Hyperdrive）也
 * 依赖 iconv-lite 的 getDecoder/getEncoder，空桩会导致 Workers 运行时报
 * "Iconv.getDecoder is not a function"。现在 Wrangler 直接打包真实 iconv-lite，
 * 本文件不再被引用，仅为兼容旧脚本保留。
 */
const unavailable = (): never => {
  throw new Error("iconv-lite is not available in this runtime")
}

export const decode = unavailable
export const encode = unavailable
export const encodingExists = () => false

export default { decode, encode, encodingExists }
