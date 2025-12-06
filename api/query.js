const axios = require("axios");
const Database = require("better-sqlite3");
const CryptoJS = require("crypto-js");

/* -------------------------------------------------------
   SESSION ENCODING / COOKIE INFO
------------------------------------------------------- */
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
  if (cookieHeader) {
    cookieHeader.split(";").forEach((cookie) => {
      const [name, value] = cookie.trim().split("=");
      if (name && value) cookies[name] = value;
    });
  }
  return cookies;
}

/* -------------------------------------------------------
   CLEAN STORE NAME
------------------------------------------------------- */
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

/* -------------------------------------------------------
   SQL ENGINE FUNCTIONS
------------------------------------------------------- */

// Build table schema dynamically from Shopify rows
function createSQLTable(db, tableName, rows) {
  if (!rows || rows.length === 0) return;

  const columns = Object.keys(rows[0])
    .map((col) => `"${col}" TEXT`)
    .join(",");

  db.exec(`CREATE TABLE ${tableName} (${columns});`);

  const insert = db.prepare(
    `INSERT INTO ${tableName} (${Object.keys(rows[0]).join(",")})
    VALUES (${Object.keys(rows[0]).map(() => "?").join(",")})`
  );

  const insertMany = db.transaction((data) => {
    for (const row of data) insert.run(Object.values(row));
  });

  insertMany(rows);
}

// Register SQL functions (SHA2, LOWER, etc.)
function registerSQLFunctions(db) {
  db.function("SHA2", (value, _bits) => {
    return CryptoJS.SHA256(String(value)).toString();
  });

  db.function("LOWER", (value) => String(value).toLowerCase());
  db.function("UPPER", (value) => String(value).toUpperCase());
  db.function("CONCAT", (...args) => args.join(""));
}

// Execute SQL on SQLite engine
async function runSQLQuery(sql, tableName, rows) {
  const db = new Database(":memory:");

  createSQLTable(db, tableName, rows);
  registerSQLFunctions(db);

  let resultRows;

  try {
    const stmt = db.prepare(sql);
    resultRows = stmt.all();
  } catch (err) {
    console.error("SQL Error:", err);
    throw new Error("SQL syntax error: " + err.message);
  }

  db.close();
  return resultRows;
}

/* -------------------------------------------------------
   MAIN API HANDLER
------------------------------------------------------- */
module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", req.headers.origin || "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Access-Control-Allow-Credentials", "true");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    const { sql, credentials, action } = req.body;
    const cookies = parseCookies(req.headers.cookie);

    /* ---------------------------------------------
        CONNECT — SAVE SHOPIFY SESSION
    --------------------------------------------- */
    if (action === "connect") {
      if (!credentials?.storeName || !credentials?.apiPassword) {
        return res.status(400).json({ error: "Missing store name or access token" });
      }

      const storeName = cleanStoreName(credentials.storeName);
      const accessToken = credentials.apiPassword.trim();

      // Test the credentials
      try {
        const testUrl = `https://${storeName}/admin/api/2024-01/shop.json`;
        await axios.get(testUrl, {
          headers: {
            "X-Shopify-Access-Token": accessToken,
            "Content-Type": "application/json",
          },
        });
      } catch (err) {
        if (err.response?.status === 401) {
          return res.status(401).json({ error: "Invalid access token" });
        }
        if (err.response?.status === 404) {
          return res.status(404).json({ error: "Store not found" });
        }
        throw err;
      }

      const sessionPayload = encodeSession({ storeName, accessToken });
      const isProduction =
        process.env.NODE_ENV === "production" || req.headers.host?.includes("vercel.app");

      const cookieOptions = `Path=/; HttpOnly; SameSite=Lax; Max-Age=86400${
        isProduction ? "; Secure" : ""
      }`;

      res.setHeader("Set-Cookie", `shopify_session=${sessionPayload}; ${cookieOptions}`);
      return res.status(200).json({ success: true, storeName });
    }

    /* ---------------------------------------------
        DISCONNECT
    --------------------------------------------- */
    if (action === "disconnect") {
      res.setHeader("Set-Cookie", "shopify_session=; Path=/; HttpOnly; Max-Age=0");
      return res.status(200).json({ success: true });
    }

    /* ---------------------------------------------
        CHECK SESSION
    --------------------------------------------- */
    if (action === "checkSession") {
      const session = decodeSession(cookies.shopify_session);
      if (session?.storeName && session?.accessToken) {
        return res.status(200).json({ connected: true, storeName: session.storeName });
      }
      return res.status(200).json({ connected: false });
    }

    /* ---------------------------------------------
        QUERY MODE — SQL EXECUTION
    --------------------------------------------- */
    if (!sql) return res.status(400).json({ error: "Missing SQL query" });

    let storeName, accessToken;

    const session = decodeSession(cookies.shopify_session);
    if (session?.storeName && session?.accessToken) {
      storeName = session.storeName;
      accessToken = session.accessToken;
    } else if (credentials?.storeName && credentials?.apiPassword) {
      storeName = cleanStoreName(credentials.storeName);
      accessToken = credentials.apiPassword.trim();
    } else {
      return res.status(401).json({ error: "Not connected. Connect your store first." });
    }

    /* ---------------------------------------------
        Shopify table mapping
    --------------------------------------------- */
    const baseUrl = `https://${storeName}/admin/api/2024-01`;
    const sqlLower = sql.trim().toLowerCase();

    let tableName;
    if (sqlLower.includes(" from orders")) tableName = "orders";
    else if (sqlLower.includes(" from products")) tableName = "products";
    else if (sqlLower.includes(" from customers")) tableName = "customers";
    else
      return res.status(400).json({
        error: `SQL must reference one of: orders, products, customers`,
      });

    const endpoints = {
      orders: "/orders.json",
      products: "/products.json",
      customers: "/customers.json",
    };

    const apiResponse = await axios.get(`${baseUrl}${endpoints[tableName]}`, {
      headers: {
        "X-Shopify-Access-Token": accessToken,
        "Content-Type": "application/json",
      },
      params: { limit: 250 },
    });

    let rows = apiResponse.data[tableName] || [];

    /* ---------------------------------------------
        Normalize Shopify → SQL row structure
    --------------------------------------------- */
    if (tableName === "orders") {
      rows = rows.map((o) => ({
        id: o.id,
        customer: `${o.customer?.first_name || ""} ${o.customer?.last_name || ""}`.trim(),
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

    /* ---------------------------------------------
        RUN FULL SQL ENGINE
    --------------------------------------------- */
    const results = await runSQLQuery(sql, tableName, rows);

    return res.status(200).json({
      results,
      count: results.length,
    });

    /* --------------------------------------------- */
  } catch (error) {
    console.error("QUERY ERROR:", error);

    if (error.response?.status === 401) {
      res.setHeader("Set-Cookie", "shopify_session=; Path=/; HttpOnly; Max-Age=0");
      return res.status(401).json({ error: "Session expired or invalid. Reconnect your store." });
    }

    return res.status(500).json({ error: error.message || "Server error" });
  }
};
