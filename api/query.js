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

// Helper to clean and validate store name
function cleanStoreName(storeName) {
  let cleaned = storeName
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')  // Remove protocol
    .replace(/\/$/, '')            // Remove trailing slash
    .replace(/\/.*$/, '');         // Remove any path
  
  // If they just entered the store name without .myshopify.com, add it
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

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  
  try {
    const { sql, credentials } = req.body;
    if (!sql || !credentials) return res.status(400).json({ error: 'Missing SQL or credentials' });
    
    const parser = new SQLParser();
    const parsed = parser.parse(sql);
    
    // Clean and validate the store name
    const storeName = cleanStoreName(credentials.storeName);
    const accessToken = credentials.apiPassword?.trim();
    
    if (!accessToken) {
      return res.status(400).json({ error: 'Missing API access token' });
    }
    
    // Use a stable API version (2024-01 is widely supported)
    const baseUrl = `https://${storeName}/admin/api/2024-01`;
    const endpoints = {
      orders: '/orders.json',
      products: '/products.json',
      customers: '/customers.json'
    };
    
    const endpoint = endpoints[parsed.table];
    if (!endpoint) return res.status(400).json({ error: `Table "${parsed.table}" not supported. Use: orders, products, or customers` });
    
    console.log(`Fetching from: ${baseUrl}${endpoint}`);
    
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
    
    // Provide helpful error messages
    if (error.response?.status === 401) {
      return res.status(401).json({ 
        error: 'Authentication failed. Please check:\n1. Your access token is correct (starts with shpat_)\n2. The app is installed on your store\n3. The token has read_orders, read_products, read_customers scopes'
      });
    }
    
    if (error.response?.status === 404) {
      return res.status(404).json({ 
        error: 'Store not found. Please check your store URL (e.g., my-store.myshopify.com)'
      });
    }
    
    if (error.response?.status === 403) {
      return res.status(403).json({ 
        error: 'Access denied. Your API token may not have the required permissions (read_orders, read_products, read_customers)'
      });
    }
    
    res.status(500).json({ error: error.response?.data?.errors || error.message });
  }
};
