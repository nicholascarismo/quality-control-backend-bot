import 'dotenv/config';
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import boltPkg from '@slack/bolt';
import { google } from 'googleapis';

const { App } = boltPkg;

/* =========================
   Constants & Env
========================= */
const {
  // Slack (Socket Mode)
  SLACK_BOT_TOKEN,
  SLACK_APP_TOKEN,
  SLACK_SIGNING_SECRET,

  // Target channel fallback for messages (if slash command channel is missing)
  WATCH_CHANNEL_ID,

  // Shopify
  SHOPIFY_DOMAIN,              // e.g. example.myshopify.com
  SHOPIFY_ADMIN_TOKEN,         // Admin API Access Token
  SHOPIFY_API_VERSION = '2025-01',

  // Google Sheets (Service Account)
  GOOGLE_SERVICE_ACCOUNT_EMAIL,
  GOOGLE_PRIVATE_KEY,          // multiline allowed; keep exact formatting with \n
  SHEET_DOC_ID,                // 1FVt...265Q  (do not commit real value)
  SHEET_TAB_NAME = 'Customer'  // exactly the tab that holds data
} = process.env;

/* =========================
   Preflight validation
========================= */
function missing(keys) {
  return keys.filter(k => !process.env[k]);
}

const missingSlack = missing(['SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN']);
if (missingSlack.length) {
  console.error('Missing required Slack env:', missingSlack.join(', '));
  process.exit(1);
}
const missingShopify = missing(['SHOPIFY_DOMAIN', 'SHOPIFY_ADMIN_TOKEN']);
if (missingShopify.length) {
  console.error('Missing required Shopify env:', missingShopify.join(', '));
  process.exit(1);
}
const missingGoogle = missing(['GOOGLE_SERVICE_ACCOUNT_EMAIL', 'GOOGLE_PRIVATE_KEY', 'SHEET_DOC_ID']);
if (missingGoogle.length) {
  console.error('Missing required Google env:', missingGoogle.join(', '));
  process.exit(1);
}

/* =========================
   Data dir & persistence
========================= */
const DATA_DIR = path.resolve('./data');
const RUN_LOG = path.join(DATA_DIR, 'run-log.json');

async function ensureDirs() {
  await fsp.mkdir(DATA_DIR, { recursive: true });
}

// Atomic JSON write
async function writeJsonAtomic(filePath, data) {
  const tmp = `${filePath}.tmp-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  await fsp.writeFile(tmp, JSON.stringify(data, null, 2), 'utf8');
  await fsp.rename(tmp, filePath);
}

async function readJsonSafe(filePath, fallback = null) {
  try {
    const txt = await fsp.readFile(filePath, 'utf8');
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}

/* =========================
   Shopify helpers (REST)
========================= */
const SHOPIFY_BASE = `https://${SHOPIFY_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}`;

let __shopifyGate = Promise.resolve();
const __SHOPIFY_MIN_GAP_MS = 400; // polite throttle for Admin REST

async function __withShopifyThrottle(fn) {
  const prev = __shopifyGate;
  let release;
  __shopifyGate = new Promise(res => { release = res; });
  await prev;
  try {
    return await fn();
  } finally {
    setTimeout(release, __SHOPIFY_MIN_GAP_MS);
  }
}

async function shopifyFetch(pathname, { method = 'GET', headers = {}, body } = {}, attempt = 1) {
  const url = `${SHOPIFY_BASE}${pathname}`;
  const res = await __withShopifyThrottle(() =>
    fetch(url, {
      method,
      headers: {
        'X-Shopify-Access-Token': SHOPIFY_ADMIN_TOKEN,
        'Content-Type': 'application/json',
        ...headers
      },
      body: body ? JSON.stringify(body) : undefined
    })
  );

  if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
    const retryAfterHeader = res.headers.get('Retry-After');
    const retryAfter = retryAfterHeader ? parseFloat(retryAfterHeader) * 1000 : Math.min(2000 * attempt, 10000);
    if (attempt <= 5) {
      console.warn(`Shopify ${res.status}. Retrying in ${retryAfter}ms (attempt ${attempt})...`);
      await new Promise(r => setTimeout(r, retryAfter));
      return shopifyFetch(pathname, { method, headers, body }, attempt + 1);
    }
  }

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Shopify ${method} ${pathname} failed: ${res.status} ${res.statusText} - ${text}`);
  }
  return res.json();
}

async function findOrderByName(orderName) {
  const encoded = encodeURIComponent(orderName); // e.g. C%234392
  const data = await shopifyFetch(`/orders.json?name=${encoded}&status=any`);
  const order = (data.orders || []).find(o => o.name === orderName);
  if (!order) throw new Error(`Order not found: ${orderName}`);
  return order;
}

async function fetchOrderMetafields(orderId) {
  const data = await shopifyFetch(`/orders/${orderId}/metafields.json`);
  const map = {};
  for (const mf of (data.metafields || [])) {
    const ns = (mf.namespace || '').trim();
    const key = (mf.key || '').trim();
    const val = (mf.value ?? '').toString();
    if (ns && key) map[`${ns}.${key}`] = val;
  }
  return map;
}

/* =========================
   Google Sheets client
========================= */
function buildSheetsClient() {
  // Handle literal "\n" in private key secrets
  const pk = (GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n');
  const auth = new google.auth.JWT({
    email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: pk,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  return google.sheets({ version: 'v4', auth });
}

async function readColumnBOrderNames(sheets) {
  // Read column B (order numbers) from the entire sheet
  const range = `'${SHEET_TAB_NAME}'!B:B`;
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_DOC_ID,
    range
  });
  const rows = resp.data.values || [];
  // rows is an array of 1-element arrays (since we selected only column B).
  // We must return [{ rowIndex: <1-based>, orderName: 'C#1234' }, ...]
  const out = [];
  for (let i = 0; i < rows.length; i++) {
    const cell = (rows[i][0] || '').toString().trim();
    if (/^C#\d{4,5}$/.test(cell)) {
      out.push({ rowIndex: i + 1, orderName: cell }); // Google Sheets rows are 1-based
    }
  }
  return out;
}

function packingSlipNotesFromThirdLine(value) {
  // Keep third line onwards; omit line1 (text) and line2 (blank)
  const lines = (value || '').split('\n');
  if (lines.length <= 2) return ''; // nothing after the first two lines
  return lines.slice(2).join('\n').trimEnd();
}

/* =========================
   Core worker
========================= */
async function processSheetAndUpdate({ client, channel, trigger_ts }) {
  const startedAt = new Date().toISOString();
  const sheets = buildSheetsClient();

  const foundOrders = await readColumnBOrderNames(sheets);
  if (!foundOrders.length) {
    await client.chat.postMessage({
      channel,
      thread_ts: trigger_ts,
      text: 'No order numbers (C#1234) found in column B of the sheet. Nothing to do.'
    });
    return;
  }

  // Fetch Shopify data per order, sequentially (keeps it simple and rate-limit safe)
  const updates = []; // { rowIndex, values: [D, E, F, G] }
  const failures = [];

  for (const item of foundOrders) {
    const { rowIndex, orderName } = item;
    try {
      const order = await findOrderByName(orderName);
      const metafields = await fetchOrderMetafields(order.id);

      const psn = packingSlipNotesFromThirdLine(metafields['custom.packing_slip_notes'] || '');
      const who = (metafields['custom.who_contacts'] || '').trim();
      const sip = (metafields['custom.ship_install_pickup'] || '').trim();
      const pif = (metafields['custom.pif_or_not'] || '').trim();

      updates.push({
        rowIndex,
        values: [psn, who, sip, pif]
      });
    } catch (e) {
      failures.push({ rowIndex, orderName, error: e.message || String(e) });
    }
  }

  // Apply updates to columns D-G for each row using batchUpdate
  if (updates.length) {
    const data = updates.map(u => ({
      range: `'${SHEET_TAB_NAME}'!D${u.rowIndex}:G${u.rowIndex}`,
      values: [u.values]
    }));
    // Chunk into groups of 400 to stay well under Sheets API limits
    const CHUNK = 400;
    for (let i = 0; i < data.length; i += CHUNK) {
      const slice = data.slice(i, i + CHUNK);
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_DOC_ID,
        requestBody: {
          valueInputOption: 'RAW',
          data: slice
        }
      });
    }
  }

  // Persist run log
  await ensureDirs();
  const log = (await readJsonSafe(RUN_LOG, [])) || [];
  log.push({
    at: startedAt,
    orders_seen: foundOrders.length,
    rows_written: updates.length,
    failures
  });
  // keep last 100 entries
  while (log.length > 100) log.shift();
  await writeJsonAtomic(RUN_LOG, log);

  // Post summary
  const lines = [];
  lines.push(`Sheet processed: ${foundOrders.length} order row(s) with C# found in column B.`);
  lines.push(`Wrote D-G on ${updates.length} row(s).`);
  if (failures.length) {
    lines.push(`Failures (${failures.length}):`);
    for (const f of failures.slice(0, 15)) {
      lines.push(`• Row ${f.rowIndex} (${f.orderName}): ${f.error}`);
    }
    if (failures.length > 15) {
      lines.push(`…and ${failures.length - 15} more`);
    }
  }
  await client.chat.postMessage({
    channel,
    thread_ts: trigger_ts,
    text: lines.join('\n')
  });
}

/* =========================
   Slack (Socket Mode)
========================= */
const app = new App({
  token: SLACK_BOT_TOKEN,
  appToken: SLACK_APP_TOKEN,
  signingSecret: SLACK_SIGNING_SECRET,
  socketMode: true,
  processBeforeResponse: true
});

app.error((err) => {
  console.error('⚠️ Bolt error:', err?.stack || err?.message || err);
});

app.command('/ping', async ({ ack, respond }) => {
  await ack();
  await respond({ text: 'pong' });
});

app.command('/qc-sheet', async ({ ack, body, client, logger }) => {
  await ack();
  try {
    const channel = WATCH_CHANNEL_ID || body.channel_id;
    const parent = await client.chat.postMessage({
      channel,
      text: 'Starting QC sync: reading Google Sheet and writing Shopify metafields into columns D–G…'
    });
    const thread_ts = parent.ts;

    // Kick off the worker
    await processSheetAndUpdate({ client, channel, trigger_ts: thread_ts });
  } catch (e) {
    logger?.error?.(e);
  }
});



/* =========================
   Start app
========================= */
(async () => {
  await ensureDirs();

  // Lightweight Google/Shopify connectivity checks (non-fatal)
  try {
    const sheets = buildSheetsClient();
    // cheap call: get spreadsheet metadata fields (no heavy data)
    await sheets.spreadsheets.get({
      spreadsheetId: SHEET_DOC_ID,
      fields: 'properties.title'
    });
    console.log('[google] sheets connectivity ok');
  } catch (e) {
    console.error('⚠️ Google Sheets check failed:', e?.message || e);
  }

  try {
    await shopifyFetch('/shop.json');
    console.log('[shopify] connectivity ok');
  } catch (e) {
    console.error('⚠️ Shopify check failed:', e?.message || e);
  }

  await app.start();
  console.log('[slack] app started (Socket Mode)');
})();