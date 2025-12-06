const axios = require('axios');

class SQLParser {
  parse(sql) {
    const query = sql.trim().toLowerCase();
    const tableMatch = query.match(/from\s+(\w+)/);
    if (!tableMatch) throw new Error('Invalid SQL: missing FROM clause');
    const table = tableMatch[1];
    
    const whereMatch = query.match(/where\s+([\s\S]+?)(?:\s+order|\s+group|\s+limit|$)/);
    const where = whereMatch ? this.parseWhere(whereMatch[1]) : null;
    
    const orderMatch = query.match(/order by\s+(\w+)(?:\s+(asc|desc))?/);
    const orderBy = orderMatch ? { field: orderMatch[1], direction: orderMatch[2] || 'asc' } : null;
    
    const limitMatch = query.match(/limit\s+(\d+)/);
    const limit = limitMatch ? parseInt(limitMatch[1]) : 250;
    
    const groupMatch = query.match(/group by\s+(\w+)/);
    const groupBy = groupMatch ? groupMatch[1] : null;
    
    return { table, where, orderBy, limit, groupBy };
  }
  
  parseWhere(whereClause) {
    const conditions = [];
    const parts = whereClause.split(/\s+(and|or)\s+/i);
    parts.forEach(part => {
      if (part.toLowerCase() === 'and' || part.toLowerCase() === 'or') return;
      const match = part.match(/(\w+)\s*(=|!=|>|<|>=|<=)\s*"?([^"]+)"?/);
      if (match) {
        conditions.push({ field: match[1], operator: match[2], value: match[3] });
      }
    });
    return conditions;
  }
}

function cleanStoreName(storeName) {
  let cleaned = storeName
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/\/$/, '')
    .replace(/\/.*$/, '');
  
  if (!cleaned.includes('.myshopify.com') && !cleaned.includes('.')) {
    cleaned = `${cleaned}.myshopify.com`;
  }
  
  return cleaned;
}

function applyFilters(data, parsed) {
  let results = [...data];
  
  if (parsed.where) {
    results = results.filter(row => parsed.where.every(c => {
      const val = row[c.field];
      const cmp = isNaN(c.value) ? c.value : parseFloat(c.value);
      switch (c.operator) {
        case '=': return val == cmp;
        case '!=': return val != cmp;
        case '>': return val > cmp;
        case '<': return val < cmp;
        case '>=': return val >= cmp;
        case '<=': return val <= cmp;
        default: return true;
      }
    }));
  }
  
  if (parsed.groupBy) {
    const grouped = {};
    results.forEach(row => {
      const key = row[parsed.groupBy];
      if (!grouped[key]) grouped[key] = [];
      grouped[key].push(row);
    });
    
    results = Object.keys(grouped).map(key => {
      const items = grouped[key];
      return {
        [parsed.groupBy]: key,
        count: items.length,
        total: items.reduce((sum, item) => sum + (item.total || 0), 0)
      };
    });
  }
  
  if (parsed.orderBy) {
    results.sort((a, b) => {
      const aVal = a[parsed.orderBy.field];
      const bVal = b[parsed.orderBy.field];
      const cmp = aVal > bVal ? 1 : aVal < bVal ? -1 : 0;
      return parsed.orderBy.direction === 'desc' ? -cmp : cmp;
    });
  }
  
  return results.slice(0, parsed.limit);
}

// Simple encoding for session data
function encodeSession(data) {
  return Buffer.from(JSON.stringify(data)).toString('base64');
}

function decodeSession(encoded) {
  try {
    return JSON.parse(Buffer.from(encoded, 'base64').toString('utf8'));
  } catch {
    return null;
  }
}

// Parse cookies from request
function parseCookies(cookieHeader) {
  const cookies = {};
  if (cookieHeader) {
    cookieHeader.split(';').forEach(cookie => {
      const [name, value] = cookie.trim().split('=');
      if (name && value) cookies[name] = value;
    });
  }
  return cookies;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', req.headers.origin || '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  
  try {
    const { sql, credentials, action } = req.body;
    const cookies = parseCookies(req.headers.cookie);
    
    // Handle connect action - store credentials in session cookie
    if (action === 'connect') {
      if (!credentials?.storeName || !credentials?.apiPassword) {
        return res.status(400).json({ error: 'Missing store name or access token' });
      }
      
      const storeName = cleanStoreName(credentials.storeName);
      const accessToken = credentials.apiPassword.trim();
      
      // Test the connection first
      try {
        const testUrl = `https://${storeName}/admin/api/2024-01/shop.json`;
        await axios.get(testUrl, {
          headers: { 
            'X-Shopify-Access-Token': accessToken,
            'Content-Type': 'application/json'
          }
        });
      } catch (error) {
        if (error.response?.status === 401) {
          return res.status(401).json({ error: 'Invalid access token. Make sure it starts with shpat_' });
        }
        if (error.response?.status === 404) {
          return res.status(404).json({ error: 'Store not found. Check your store URL.' });
        }
        throw error;
      }
      
      // Create session
      const sessionData = encodeSession({ storeName, accessToken });
      
      // Set HTTP-only cookie (secure, persists across refreshes)
      const isProduction = process.env.NODE_ENV === 'production' || req.headers.host?.includes('vercel.app');
      const cookieOptions = `Path=/; HttpOnly; SameSite=Lax; Max-Age=86400${isProduction ? '; Secure' : ''}`;
      res.setHeader('Set-Cookie', `shopify_session=${sessionData}; ${cookieOptions}`);
      
      return res.status(200).json({ success: true, storeName });
    }
    
    // Handle disconnect action
    if (action === 'disconnect') {
      res.setHeader('Set-Cookie', 'shopify_session=; Path=/; HttpOnly; Max-Age=0');
      return res.status(200).json({ success: true });
    }
    
    // Handle check session action
    if (action === 'checkSession') {
      const session = decodeSession(cookies.shopify_session);
      if (session?.storeName && session?.accessToken) {
        return res.status(200).json({ connected: true, storeName: session.storeName });
      }
      return res.status(200).json({ connected: false });
    }
    
    // Handle query action
    if (!sql) return res.status(400).json({ error: 'Missing SQL query' });
    
    // Get credentials from session cookie
    let storeName, accessToken;
    
    const session = decodeSession(cookies.shopify_session);
    if (session?.storeName && session?.accessToken) {
      storeName = session.storeName;
      accessToken = session.accessToken;
    } else if (credentials?.storeName && credentials?.apiPassword) {
      // Fallback to request body (for backwards compatibility)
      storeName = cleanStoreName(credentials.storeName);
      accessToken = credentials.apiPassword.trim();
    } else {
      return res.status(401).json({ error: 'Not connected. Please connect your store first.' });
    }
    
    const parser = new SQLParser();
    const parsed = parser.parse(sql);
    
    const baseUrl = `https://${storeName}/admin/api/2024-01`;
    const endpoints = {
      orders: '/orders.json',
      products: '/products.json',
      customers: '/customers.json'
    };
    
    const endpoint = endpoints[parsed.table];
    if (!endpoint) return res.status(400).json({ error: `Table "${parsed.table}" not supported. Use: orders, products, or customers` });
    
    const response = await axios.get(`${baseUrl}${endpoint}`, {
      headers: { 
        'X-Shopify-Access-Token': accessToken,
        'Content-Type': 'application/json'
      },
      params: { limit: 250 }
    });
    
    let data = response.data[parsed.table] || [];
    
    if (parsed.table === 'orders') {
      data = data.map(o => ({
        id: o.id,
        customer: `${o.customer?.first_name || ''} ${o.customer?.last_name || ''}`.trim() || 'Guest',
        email: o.email || o.customer?.email || '',
        total: parseFloat(o.total_price),
        date: o.created_at.split('T')[0],
        status: o.fulfillment_status || 'unfulfilled',
        items: o.line_items?.length || 0
      }));
    } else if (parsed.table === 'products') {
      data = data.map(p => ({
        id: p.id,
        title: p.title,
        price: parseFloat(p.variants[0]?.price || 0),
        inventory: p.variants[0]?.inventory_quantity || 0,
        vendor: p.vendor || '',
        sku: p.variants[0]?.sku || ''
      }));
    } else if (parsed.table === 'customers') {
      data = data.map(c => ({
        id: c.id,
        name: `${c.first_name || ''} ${c.last_name || ''}`.trim(),
        email: c.email,
        orders: c.orders_count || 0,
        total_spent: parseFloat(c.total_spent || 0),
        location: c.default_address?.city || '',
        created: c.created_at.split('T')[0]
      }));
    }
    
    const results = applyFilters(data, parsed);
    res.status(200).json({ results, count: results.length });
    
  } catch (error) {
    console.error('Query error:', error.response?.data || error.message);
    
    if (error.response?.status === 401) {
      res.setHeader('Set-Cookie', 'shopify_session=; Path=/; HttpOnly; Max-Age=0');
      return res.status(401).json({ 
        error: 'Session expired or invalid. Please reconnect your store.'
      });
    }
    
    if (error.response?.status === 404) {
      return res.status(404).json({ error: 'Store not found.' });
    }
    
    if (error.response?.status === 403) {
      return res.status(403).json({ error: 'Access denied. Check API permissions.' });
    }
    
    res.status(500).json({ error: error.response?.data?.errors || error.message });
  }
};
