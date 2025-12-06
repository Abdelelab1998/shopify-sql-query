const axios = require("axios");
const duckdb = require("@duckdb/duckdb-wasm");
const CryptoJS = require("crypto-js");

/* -----------------------------------------------
   DUCKDB ENGINE SETUP
------------------------------------------------ */
const DUCKDB_BUNDLES = duckdb.getJsDelivrBundles();
const bundle = DUCKDB_BUNDLES["duckdb-wasm-esm"];

async function runSQLQuery(sql, tableName, rows) {
  if (!rows || rows.length === 0) return [];

  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, bundle.mainModule, bundle.mainWorker);
  await db.instantiate();
  const conn = await db.connect();

  // Create table dynamically
  const columns = Object.keys(rows[0])
    .map((c) => `"${c}" VARCHAR`)
    .join(",");

  await conn.query(`CREATE TABLE ${tableName} (${columns});`);

  // Insert rows
  for (const row of rows) {
    const colNames = Object.keys(row).join(",");
    const values = Object.values(row)
      .map((v) => `'${String(v ?? "").replace(/'/g, "''")}'`)
      .join(",");

    await conn.query(
      `INSERT INTO ${tableName} (${colNames}) VALUES (${values});`
    );
  }

  // Register SHA2 hashing function
  await conn.register_udf("SHA2", (value) => {
    return CryptoJS.SHA256(String(value)).toString();
  });

  // Execute SQL
  try {
    const result = await conn.query(sql);
    return result.toArray();
  } catch (err) {
    throw new Error("SQL Error: " + err.message);
  }
}

/* -----------------------------------------------
   SESSION HANDLING
------------------------------------------------ */
function encodeSession(data) {
  return Buffer.from(JSON.stringify(data)).toString("base64");
}

function decodeSession(encoded) {
  try {
    return JSON.parse(Buffer.from(encoded, "base64").toString("utf8"));
  } catch {
    return null;
  }
}

function parseCookies(cookieHeader) {
  const cookies = {};
  if (!cookieHeader) return cookies;

  cookieHeader.split(";").forEach((cookie) => {
    const [name, value] = cookie.trim().split("=");
    if (name && value) cookies[name] = value;
  });

  return cookies;
}

/* -----------------------------------------------
   CLEAN SHOPIFY STORE NAME
------------------------------------------------ */
function cleanStoreName(storeName) {
  let cleaned = storeName
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/\/$/, "")
    .replace(/\/.*/, "");

  if (!cleaned.includes(".myshopify.com") && !cleaned.includes(".")) {
    cleaned = `${cleaned}.myshopify.com`;
  }

  return cleaned;
}

/* -----------------------------------------------
   MAIN API HANDLER
------------------------------------------------ */
module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", req.headers.origin || "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Access-Control-Allow-Credentials", "true");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST")
    return res.status(405).json({ error: "Method not allowed" });

  try {
    const { sql, credentials, action } = req.body;
    const cookies = parseCookies(req.headers.cookie);

    /* -----------------------------------------------
       CONNECT SHOPIFY STORE
    ------------------------------------------------ */
    if (action === "connect") {
      if (!credentials?.storeName || !credentials?.apiPassword) {
        return res
          .status(400)
          .json({ error: "Missing store name or access token" });
      }

      const storeName = cleanStoreName(credentials.storeName);
      const accessToken = credentials.apiPassword.trim();

      // Validate connection
      try {
        await axios.get(
          `https://${storeName}/admin/api/2024-01/shop.json`,
          {
            headers: {
              "X-Shopify-Access-Token": accessToken,
              "Content-Type": "application/json",
            },
          }
        );
      } catch (err) {
        if (err.response?.status === 401)
          return res.status(401).json({ error: "Invalid access token" });
        if (err.response?.status === 404)
          return res.status(404).json({ error: "Store not found" });
        throw err;
      }

      const sessionEnc = encodeSession({ storeName, accessToken });
      const isProd =
        process.env.NODE_ENV === "production" ||
        req.headers.host?.includes("vercel.app");

      res.setHeader(
        "Set-Cookie",
        `shopify_session=${sessionEnc}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400${
          isProd ? "; Secure" : ""
        }`
      );

      return res.status(200).json({ success: true, storeName });
    }

    /* -----------------------------------------------
       DISCONNECT
    ------------------------------------------------ */
    if (action === "disconnect") {
      res.setHeader(
        "Set-Cookie",
        "shopify_session=; Path=/; HttpOnly; Max-Age=0"
      );
      return res.status(200).json({ success: true });
    }

    /* -----------------------------------------------
       CHECK SESSION
    ------------------------------------------------ */
    if (action === "checkSession") {
      const session = decodeSession(cookies.shopify_session);
      if (session?.storeName && session?.accessToken) {
        return res
          .status(200)
          .json({ connected: true, storeName: session.storeName });
      }
      return res.status(200).json({ connected: false });
    }

    /* -----------------------------------------------
       RUN SQL QUERY
    ------------------------------------------------ */
    if (!sql)
      return res.status(400).json({ error: "SQL query is required" });

    let storeName, accessToken;

    const session = decodeSession(cookies.shopify_session);
    if (session) {
      storeName = session.storeName;
      accessToken = session.accessToken;
    } else if (credentials?.storeName && credentials?.apiPassword) {
      storeName = cleanStoreName(credentials.storeName);
      accessToken = credentials.apiPassword.trim();
    } else {
      return res
        .status(401)
        .json({ error: "Not connected. Please connect first." });
    }

    /* -----------------------------------------------
       DETECT TABLE FROM SQL
    ------------------------------------------------ */
    const tableMatch = sql.toLowerCase().match(
      /from\s+(orders|products|customers)/
    );
    if (!tableMatch)
      return res.status(400).json({
        error: "SQL must reference one of: orders, products, customers",
      });

    const tableName = tableMatch[1];

    /* -----------------------------------------------
       FETCH SHOPIFY DATA
    ------------------------------------------------ */
    const baseUrl = `https://${storeName}/admin/api/2024-01`;
    const endpoint = {
      orders: "/orders.json",
      products: "/products.json",
      customers: "/customers.json",
    }[tableName];

    const apiData = await axios.get(`${baseUrl}${endpoint}`, {
      headers: {
        "X-Shopify-Access-Token": accessToken,
        "Content-Type": "application/json",
      },
      params: { limit: 250 },
    });

    let rows = apiData.data[tableName] || [];

    // Normalize data into SQL-friendly flat objects
    if (tableName === "orders") {
      rows = rows.map((o) => ({
        id: o.id,
        customer:
          `${o.customer?.first_name || ""} ${o.customer?.last_name || ""}`.trim(),
        email: o.email || o.customer?.email || "",
        total: o.total_price,
        date: o.created_at?.split("T")[0],
        status: o.fulfillment_status || "unfulfilled",
        items: o.line_items?.length || 0,
      }));
    }

    if (tableName === "products") {
      rows = rows.map((p) => ({
        id: p.id,
        title: p.title,
        price: p.variants?.[0]?.price,
        inventory: p.variants?.[0]?.inventory_quantity,
        vendor: p.vendor,
        sku: p.variants?.[0]?.sku,
      }));
    }

    if (tableName === "customers") {
      rows = rows.map((c) => ({
        id: c.id,
        name: `${c.first_name || ""} ${c.last_name || ""}`.trim(),
        email: c.email,
        orders: c.orders_count,
        total_spent: c.total_spent,
        location: c.default_address?.city,
        created: c.created_at?.split("T")[0],
      }));
    }

    /* -----------------------------------------------
       EXECUTE SQL USING DUCKDB
    ------------------------------------------------ */
    const results = await runSQLQuery(sql, tableName, rows);

    return res.status(200).json({
      results,
      count: results.length,
    });
  } catch (err) {
    console.error("SERVER ERROR:", err);
    return res
      .status(500)
      .json({ error: err.message || "Internal Server Error" });
  }
};
