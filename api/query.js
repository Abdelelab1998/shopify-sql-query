const axios = require('axios');
const alasql = require('alasql');

// Add custom SQL functions
alasql.fn.SHA2 = function(str, bits) {
  if (!str) return null;
  const crypto = require('crypto');
  const algorithm = bits === 256 ? 'sha256' : bits === 512 ? 'sha512' : 'sha256';
  return crypto.createHash(algorithm).update(String(str)).digest('hex');
};

alasql.fn.MD5 = function(str) {
  if (!str) return null;
  const crypto = require('crypto');
  return crypto.createHash('md5').update(String(str)).digest('hex');
};

alasql.fn.LOWER = function(str) {
  return str ? String(str).toLowerCase() : null;
};

alasql.fn.UPPER = function(str) {
  return str ? String(str).toUpperCase() : null;
};

alasql.fn.CONCAT = function(...args) {
  return args.filter(a => a != null).join('');
};

alasql.fn.SUBSTRING = function(str, start, length) {
  if (!str) return null;
  return String(str).substring(start - 1, length ? start - 1 + length : undefined);
};

alasql.fn.TRIM = function(str) {
  return str ? String(str).trim() : null;
};

alasql.fn.LENGTH = function(str) {
  return str ? String(str).length : 0;
};

alasql.fn.NOW = function() {
  return new Date().toISOString();
};

alasql.fn.DATE = function(str) {
  if (!str) return null;
  return new Date(str).toISOString().split('T')[0];
};

alasql.fn.YEAR = function(str) {
  if (!str) return null;
  return new Date(str).getFullYear();
};

alasql.fn.MONTH = function(str) {
  if (!str) return null;
  return new Date(str).getMonth() + 1;
};

alasql.fn.DAY = function(str) {
  if (!str) return null;
  return new Date(str).getDate();
};

alasql.fn.COALESCE = function(...args) {
  for (const arg of args) {
    if (arg != null) return arg;
  }
  return null;
};

alasql.fn.IFNULL = function(val, defaultVal) {
  return val != null ? val : defaultVal;
};

alasql.fn.NULLIF = function(val1, val2) {
  return val1 === val2 ? null : val1;
};

alasql.fn.CAST = function(val, type) {
  if (val == null) return null;
  switch(type?.toUpperCase()) {
    case 'INT':
    case 'INTEGER':
      return parseInt(val, 10);
    case 'FLOAT':
    case 'DECIMAL':
    case 'DOUBLE':
      return parseFloat(val);
    case 'STRING':
    case 'VARCHAR':
    case 'CHAR':
      return String(val);
    default:
      return val;
  }
};

// Helper functions
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

// GraphQL queries for each resource
const graphqlQueries = {
  orders: `
    query($first: Int!, $after: String) {
      orders(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          name
          email
          createdAt
          updatedAt
          cancelledAt
          closedAt
          processedAt
          totalPriceSet { shopMoney { amount currencyCode } }
          subtotalPriceSet { shopMoney { amount currencyCode } }
          totalTaxSet { shopMoney { amount currencyCode } }
          totalShippingPriceSet { shopMoney { amount currencyCode } }
          totalDiscountsSet { shopMoney { amount currencyCode } }
          totalRefundedSet { shopMoney { amount currencyCode } }
          displayFinancialStatus
          displayFulfillmentStatus
          fulfillable
          note
          tags
          sourceName
          customerJourneySummary {
            ready
            daysToConversion
            momentsCount { count }
            firstVisit {
              occurredAt
              landingPage
              referrerUrl
              source
              sourceType
              referralCode
              utmParameters {
                source
                medium
                campaign
                content
                term
              }
            }
            lastVisit {
              occurredAt
              landingPage
              referrerUrl
              source
              sourceType
              referralCode
              utmParameters {
                source
                medium
                campaign
                content
                term
              }
            }
          }
          customer { id firstName lastName email phone }
          shippingAddress { address1 address2 city province country zip }
          billingAddress { address1 address2 city province country zip }
          lineItems(first: 50) { nodes { id title quantity sku vendor 
            originalUnitPriceSet { shopMoney { amount } }
            discountedUnitPriceSet { shopMoney { amount } }
            variant { id title sku }
            product { id title }
          }}
        }
      }
    }
  `,
  
  products: `
    query($first: Int!, $after: String) {
      products(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          title
          handle
          descriptionHtml
          description
          vendor
          productType
          tags
          status
          createdAt
          updatedAt
          publishedAt
          totalInventory
          tracksInventory
          priceRangeV2 {
            minVariantPrice { amount currencyCode }
            maxVariantPrice { amount currencyCode }
          }
          options { name values }
          variants(first: 100) { 
            nodes { 
              id title sku barcode price compareAtPrice
              inventoryQuantity availableForSale
              weight weightUnit
              selectedOptions { name value }
              inventoryItem { id tracked }
            } 
          }
          images(first: 10) { nodes { url altText } }
          featuredImage { url altText }
          seo { title description }
          collections(first: 10) { nodes { id title } }
        }
      }
    }
  `,
  
  customers: `
    query($first: Int!, $after: String) {
      customers(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          firstName
          lastName
          email
          phone
          createdAt
          updatedAt
          note
          tags
          state
          taxExempt
          verifiedEmail
          validEmailAddress
          numberOfOrders
          amountSpent { amount currencyCode }
          defaultAddress { 
            address1 address2 city province provinceCode country countryCodeV2 zip 
            company phone
          }
        }
      }
    }
  `,
  
  collections: `
    query($first: Int!, $after: String) {
      collections(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          title
          handle
          description
          descriptionHtml
          sortOrder
          productsCount { count }
          updatedAt
          image { url altText }
          seo { title description }
        }
      }
    }
  `,
  
  inventory_items: `
    query($first: Int!, $after: String) {
      inventoryItems(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          sku
          tracked
          createdAt
          updatedAt
          countryCodeOfOrigin
          provinceCodeOfOrigin
          harmonizedSystemCode
          inventoryLevels(first: 10) {
            nodes {
              id
              available
              location { id name }
            }
          }
          variant { id title product { id title } }
        }
      }
    }
  `,
  
  locations: `
    query {
      locations(first: 50) {
        nodes {
          id
          name
          address { address1 address2 city province country zip }
          isActive
          fulfillsOnlineOrders
          hasActiveInventory
        }
      }
    }
  `,
  
  shop: `
    query {
      shop {
        id
        name
        email
        myshopifyDomain
        primaryDomain { url host }
        currencyCode
        weightUnit
        timezoneAbbreviation
        billingAddress { address1 city province country zip }
        plan { displayName partnerDevelopment shopifyPlus }
      }
    }
  `
};

// Transform GraphQL responses to flat table format
function transformToTable(resource, data) {
  switch(resource) {
    case 'orders':
      return data.map(o => {
        const journey = o.customerJourneySummary;
        const firstVisit = journey?.firstVisit;
        const lastVisit = journey?.lastVisit;
        
        return {
          // Order identifiers
          id: o.id?.split('/').pop(),
          order_number: o.name,
          email: o.email,
          
          // Timestamps
          created_at: o.createdAt,
          updated_at: o.updatedAt,
          cancelled_at: o.cancelledAt,
          closed_at: o.closedAt,
          processed_at: o.processedAt,
          
          // Financial
          total_price: parseFloat(o.totalPriceSet?.shopMoney?.amount || 0),
          subtotal_price: parseFloat(o.subtotalPriceSet?.shopMoney?.amount || 0),
          total_tax: parseFloat(o.totalTaxSet?.shopMoney?.amount || 0),
          total_shipping: parseFloat(o.totalShippingPriceSet?.shopMoney?.amount || 0),
          total_discounts: parseFloat(o.totalDiscountsSet?.shopMoney?.amount || 0),
          total_refunded: parseFloat(o.totalRefundedSet?.shopMoney?.amount || 0),
          currency: o.totalPriceSet?.shopMoney?.currencyCode,
          
          // Status
          financial_status: o.displayFinancialStatus,
          fulfillment_status: o.displayFulfillmentStatus,
          fulfillable: o.fulfillable,
          
          // Misc
          note: o.note,
          tags: o.tags?.join(', '),
          source_name: o.sourceName,
          line_items_count: o.lineItems?.nodes?.length || 0,
          
          // Customer Journey
          journey_ready: journey?.ready,
          days_to_conversion: journey?.daysToConversion,
          touchpoints_count: journey?.momentsCount?.count,
          
          // First Visit (Acquisition)
          first_visit_at: firstVisit?.occurredAt,
          first_landing_page: firstVisit?.landingPage,
          first_referrer: firstVisit?.referrerUrl,
          first_source: firstVisit?.source,
          first_source_type: firstVisit?.sourceType,
          first_referral_code: firstVisit?.referralCode,
          first_utm_source: firstVisit?.utmParameters?.source,
          first_utm_medium: firstVisit?.utmParameters?.medium,
          first_utm_campaign: firstVisit?.utmParameters?.campaign,
          first_utm_content: firstVisit?.utmParameters?.content,
          first_utm_term: firstVisit?.utmParameters?.term,
          
          // Last Visit (Conversion)
          last_visit_at: lastVisit?.occurredAt,
          last_landing_page: lastVisit?.landingPage,
          last_referrer: lastVisit?.referrerUrl,
          last_source: lastVisit?.source,
          last_source_type: lastVisit?.sourceType,
          last_referral_code: lastVisit?.referralCode,
          last_utm_source: lastVisit?.utmParameters?.source,
          last_utm_medium: lastVisit?.utmParameters?.medium,
          last_utm_campaign: lastVisit?.utmParameters?.campaign,
          last_utm_content: lastVisit?.utmParameters?.content,
          last_utm_term: lastVisit?.utmParameters?.term,
          
          // Customer
          customer_id: o.customer?.id?.split('/').pop(),
          customer_email: o.customer?.email,
          customer_first_name: o.customer?.firstName,
          customer_last_name: o.customer?.lastName,
          customer_phone: o.customer?.phone,
          
          // Shipping Address
          shipping_address1: o.shippingAddress?.address1,
          shipping_address2: o.shippingAddress?.address2,
          shipping_city: o.shippingAddress?.city,
          shipping_province: o.shippingAddress?.province,
          shipping_country: o.shippingAddress?.country,
          shipping_zip: o.shippingAddress?.zip,
          
          // Billing Address
          billing_address1: o.billingAddress?.address1,
          billing_address2: o.billingAddress?.address2,
          billing_city: o.billingAddress?.city,
          billing_province: o.billingAddress?.province,
          billing_country: o.billingAddress?.country,
          billing_zip: o.billingAddress?.zip
        };
      });
      
    case 'order_line_items':
      const lineItems = [];
      data.forEach(o => {
        (o.lineItems?.nodes || []).forEach(li => {
          lineItems.push({
            id: li.id?.split('/').pop(),
            order_id: o.id?.split('/').pop(),
            order_number: o.name,
            title: li.title,
            quantity: li.quantity,
            sku: li.sku || li.variant?.sku,
            vendor: li.vendor,
            unit_price: parseFloat(li.originalUnitPriceSet?.shopMoney?.amount || 0),
            discounted_price: parseFloat(li.discountedUnitPriceSet?.shopMoney?.amount || 0),
            variant_id: li.variant?.id?.split('/').pop(),
            variant_title: li.variant?.title,
            product_id: li.product?.id?.split('/').pop(),
            product_title: li.product?.title
          });
        });
      });
      return lineItems;
      
    case 'products':
      return data.map(p => ({
        id: p.id?.split('/').pop(),
        title: p.title,
        handle: p.handle,
        description: p.description,
        vendor: p.vendor,
        product_type: p.productType,
        tags: p.tags?.join(', '),
        status: p.status,
        created_at: p.createdAt,
        updated_at: p.updatedAt,
        published_at: p.publishedAt,
        total_inventory: p.totalInventory,
        tracks_inventory: p.tracksInventory,
        min_price: parseFloat(p.priceRangeV2?.minVariantPrice?.amount || 0),
        max_price: parseFloat(p.priceRangeV2?.maxVariantPrice?.amount || 0),
        currency: p.priceRangeV2?.minVariantPrice?.currencyCode,
        options: p.options?.map(o => o.name).join(', '),
        image_url: p.featuredImage?.url,
        seo_title: p.seo?.title,
        seo_description: p.seo?.description,
        collections: p.collections?.nodes?.map(c => c.title).join(', '),
        variants_count: p.variants?.nodes?.length || 0
      }));
      
    case 'product_variants':
      const variants = [];
      data.forEach(p => {
        (p.variants?.nodes || []).forEach(v => {
          variants.push({
            id: v.id?.split('/').pop(),
            product_id: p.id?.split('/').pop(),
            product_title: p.title,
            title: v.title,
            sku: v.sku,
            barcode: v.barcode,
            price: parseFloat(v.price || 0),
            compare_at_price: v.compareAtPrice ? parseFloat(v.compareAtPrice) : null,
            inventory_quantity: v.inventoryQuantity,
            available_for_sale: v.availableForSale,
            weight: v.weight,
            weight_unit: v.weightUnit,
            options: v.selectedOptions?.map(o => `${o.name}: ${o.value}`).join(', '),
            inventory_item_id: v.inventoryItem?.id?.split('/').pop(),
            inventory_tracked: v.inventoryItem?.tracked
          });
        });
      });
      return variants;
      
    case 'customers':
      return data.map(c => ({
        id: c.id?.split('/').pop(),
        email: c.email,
        first_name: c.firstName,
        last_name: c.lastName,
        full_name: [c.firstName, c.lastName].filter(Boolean).join(' '),
        phone: c.phone,
        created_at: c.createdAt,
        updated_at: c.updatedAt,
        note: c.note,
        tags: c.tags?.join(', '),
        state: c.state,
        tax_exempt: c.taxExempt,
        verified_email: c.verifiedEmail,
        valid_email: c.validEmailAddress,
        orders_count: c.numberOfOrders,
        total_spent: parseFloat(c.amountSpent?.amount || 0),
        currency: c.amountSpent?.currencyCode,
        address1: c.defaultAddress?.address1,
        address2: c.defaultAddress?.address2,
        city: c.defaultAddress?.city,
        province: c.defaultAddress?.province,
        province_code: c.defaultAddress?.provinceCode,
        country: c.defaultAddress?.country,
        country_code: c.defaultAddress?.countryCodeV2,
        zip: c.defaultAddress?.zip,
        company: c.defaultAddress?.company
      }));
      
    case 'collections':
      return data.map(c => ({
        id: c.id?.split('/').pop(),
        title: c.title,
        handle: c.handle,
        description: c.description,
        sort_order: c.sortOrder,
        products_count: c.productsCount?.count,
        updated_at: c.updatedAt,
        image_url: c.image?.url,
        seo_title: c.seo?.title,
        seo_description: c.seo?.description
      }));
      
    case 'inventory_items':
      return data.map(i => ({
        id: i.id?.split('/').pop(),
        sku: i.sku,
        tracked: i.tracked,
        created_at: i.createdAt,
        updated_at: i.updatedAt,
        country_of_origin: i.countryCodeOfOrigin,
        province_of_origin: i.provinceCodeOfOrigin,
        hs_code: i.harmonizedSystemCode,
        variant_id: i.variant?.id?.split('/').pop(),
        variant_title: i.variant?.title,
        product_id: i.variant?.product?.id?.split('/').pop(),
        product_title: i.variant?.product?.title
      }));
      
    case 'inventory_levels':
      const levels = [];
      data.forEach(i => {
        (i.inventoryLevels?.nodes || []).forEach(l => {
          levels.push({
            id: l.id?.split('/').pop(),
            inventory_item_id: i.id?.split('/').pop(),
            sku: i.sku,
            location_id: l.location?.id?.split('/').pop(),
            location_name: l.location?.name,
            available: l.available,
            product_title: i.variant?.product?.title,
            variant_title: i.variant?.title
          });
        });
      });
      return levels;
      
    case 'locations':
      return data.map(l => ({
        id: l.id?.split('/').pop(),
        name: l.name,
        address1: l.address?.address1,
        address2: l.address?.address2,
        city: l.address?.city,
        province: l.address?.province,
        country: l.address?.country,
        zip: l.address?.zip,
        is_active: l.isActive,
        fulfills_online_orders: l.fulfillsOnlineOrders,
        has_active_inventory: l.hasActiveInventory
      }));
      
    case 'shop':
      const s = data;
      return [{
        id: s.id?.split('/').pop(),
        name: s.name,
        email: s.email,
        myshopify_domain: s.myshopifyDomain,
        domain: s.primaryDomain?.url,
        currency: s.currencyCode,
        weight_unit: s.weightUnit,
        timezone: s.timezoneAbbreviation,
        plan_name: s.plan?.displayName,
        is_partner_dev: s.plan?.partnerDevelopment,
        is_plus: s.plan?.shopifyPlus
      }];
      
    default:
      return data;
  }
}

// Fetch data from Shopify GraphQL API
async function fetchShopifyData(storeName, accessToken, resource, maxRecords = 250) {
  const sourceMap = {
    'order_line_items': 'orders',
    'product_variants': 'products',
    'inventory_levels': 'inventory_items'
  };
  
  const baseResource = sourceMap[resource] || resource;
  const query = graphqlQueries[baseResource];
  
  if (!query) {
    throw new Error(`Unknown table: ${resource}`);
  }
  
  const url = `https://${storeName}/admin/api/2024-10/graphql.json`;
  let allData = [];
  let hasNextPage = true;
  let cursor = null;
  
  // For non-paginated queries (shop, locations)
  if (!query.includes('$first')) {
    const response = await axios.post(url, { query }, {
      headers: {
        'X-Shopify-Access-Token': accessToken,
        'Content-Type': 'application/json'
      }
    });
    
    if (response.data.errors) {
      throw new Error(response.data.errors[0]?.message || 'GraphQL error');
    }
    
    const key = Object.keys(response.data.data)[0];
    const data = response.data.data[key];
    return transformToTable(resource, data.nodes || [data]);
  }
  
  // Paginated queries
  while (hasNextPage && allData.length < maxRecords) {
    const variables = { first: Math.min(50, maxRecords - allData.length), after: cursor };
    
    const response = await axios.post(url, { query, variables }, {
      headers: {
        'X-Shopify-Access-Token': accessToken,
        'Content-Type': 'application/json'
      }
    });
    
    if (response.data.errors) {
      throw new Error(response.data.errors[0]?.message || 'GraphQL error');
    }
    
    const key = Object.keys(response.data.data)[0];
    const result = response.data.data[key];
    allData = allData.concat(result.nodes || []);
    hasNextPage = result.pageInfo?.hasNextPage && allData.length < maxRecords;
    cursor = result.pageInfo?.endCursor;
  }
  
  return transformToTable(resource, allData);
}

// Detect which tables are needed from SQL query
function detectTablesFromSQL(sql) {
  const tables = [];
  const normalizedSQL = sql.toLowerCase();
  
  const availableTables = [
    'orders', 'order_line_items',
    'products', 'product_variants',
    'customers',
    'collections',
    'inventory_items', 'inventory_levels',
    'locations',
    'shop'
  ];
  
  for (const table of availableTables) {
    const patterns = [
      new RegExp(`\\bfrom\\s+${table}\\b`, 'i'),
      new RegExp(`\\bjoin\\s+${table}\\b`, 'i'),
      new RegExp(`\\b${table}\\.`, 'i'),
      new RegExp(`"${table}"`, 'i')
    ];
    
    if (patterns.some(p => p.test(normalizedSQL))) {
      tables.push(table);
    }
  }
  
  return [...new Set(tables)];
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
    
    // Handle connect action
    if (action === 'connect') {
      if (!credentials?.storeName || !credentials?.apiPassword) {
        return res.status(400).json({ error: 'Missing store name or access token' });
      }
      
      const storeName = cleanStoreName(credentials.storeName);
      const accessToken = credentials.apiPassword.trim();
      
      try {
        const testQuery = `query { shop { name } }`;
        await axios.post(`https://${storeName}/admin/api/2024-10/graphql.json`, 
          { query: testQuery },
          {
            headers: { 
              'X-Shopify-Access-Token': accessToken,
              'Content-Type': 'application/json'
            }
          }
        );
      } catch (error) {
        if (error.response?.status === 401) {
          return res.status(401).json({ error: 'Invalid access token. Make sure it starts with shpat_' });
        }
        if (error.response?.status === 404) {
          return res.status(404).json({ error: 'Store not found. Check your store URL.' });
        }
        throw error;
      }
      
      const sessionData = encodeSession({ storeName, accessToken });
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
    
    // Handle SQL query
    if (!sql) return res.status(400).json({ error: 'Missing SQL query' });
    
    let storeName, accessToken;
    const session = decodeSession(cookies.shopify_session);
    if (session?.storeName && session?.accessToken) {
      storeName = session.storeName;
      accessToken = session.accessToken;
    } else if (credentials?.storeName && credentials?.apiPassword) {
      storeName = cleanStoreName(credentials.storeName);
      accessToken = credentials.apiPassword.trim();
    } else {
      return res.status(401).json({ error: 'Not connected. Please connect your store first.' });
    }
    
    const tables = detectTablesFromSQL(sql);
    
    if (tables.length === 0) {
      return res.status(400).json({ 
        error: 'No valid table found. Available: orders, order_line_items, products, product_variants, customers, collections, inventory_items, inventory_levels, locations, shop'
      });
    }
    
    const tableData = {};
    for (const table of tables) {
      try {
        tableData[table] = await fetchShopifyData(storeName, accessToken, table);
      } catch (error) {
        console.error(`Error fetching ${table}:`, error.message);
        throw new Error(`Failed to fetch ${table}: ${error.message}`);
      }
    }
    
    for (const [tableName, data] of Object.entries(tableData)) {
      alasql(`DROP TABLE IF EXISTS ${tableName}`);
      alasql(`CREATE TABLE ${tableName}`);
      alasql.tables[tableName].data = data;
    }
    
    let results;
    try {
      results = alasql(sql);
    } catch (sqlError) {
      let errorMsg = sqlError.message;
      if (errorMsg.includes('not found')) {
        errorMsg += '. Available tables: ' + Object.keys(tableData).join(', ');
      }
      return res.status(400).json({ error: `SQL Error: ${errorMsg}` });
    }
    
    if (!Array.isArray(results)) {
      results = [results];
    }
    
    res.status(200).json({ results, count: results.length });
    
  } catch (error) {
    console.error('Query error:', error.response?.data || error.message);
    
    if (error.response?.status === 401) {
      res.setHeader('Set-Cookie', 'shopify_session=; Path=/; HttpOnly; Max-Age=0');
      return res.status(401).json({ error: 'Session expired. Please reconnect.' });
    }
    
    res.status(500).json({ error: error.message });
  }
};
