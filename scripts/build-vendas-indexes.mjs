import fs from 'node:fs/promises';
import path from 'node:path';

const FIREBASE_URL = 'https://justburger-producao-e9b27-default-rtdb.firebaseio.com';
const LEGACY_REPORTS_URL = `${FIREBASE_URL}/relatorios_diarios.json`;
const INDEXED_META_COMPANIES_URL = `${FIREBASE_URL}/vendas_indexadas/meta/companies.json`;
const INDEXED_COMPANY_URL = slug => `${FIREBASE_URL}/vendas_indexadas/companies/${slug}.json`;
const OUTPUT_ROOT = process.argv[2] || 'data/vendas-index';
const STORE_ORDER = ['Burger Matriz','Burger ABC','Burger GRU','Gourmet','Aham Forneria','Aham Sushi'];
const DEFAULT_STORE_SLUGS = Object.fromEntries(STORE_ORDER.map(name => [name, slugify(name)]));
const LOJA_CODIGO_MAP = {
  '21870':'Burger Matriz',
  'aa6576e8-574e-42c5-8d38-0d7df31f3fcc':'Burger ABC',
  '8432b620-27ea-48fb-bf3b-a0b76bbf3516':'Burger GRU',
  '510cb9b9-a4dc-4962-a70f-cc01b0e4bdc8':'Aham Forneria',
  '1eb0ccfe-ba7e-43ad-9a23-06c4ea7a823a':'Aham Forneria',
  'fe4e06bd-2f30-410a-97ef-b21eba59f3e4':'Aham Sushi',
  '20215':'Aham Sushi'
};

function slugify(v=''){
  return String(v || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'');
}
function ymd(d){
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
function monthKey(d){
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
}
function monthStart(monthKeyStr){
  const [y,m] = String(monthKeyStr).split('-').map(Number);
  return new Date(y, m - 1, 1);
}
function parseDateFlex(value){
  if(value == null || value === '') return null;
  if(value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  if(typeof value === 'number'){
    const epoch = new Date(Math.round((value - 25569) * 86400 * 1000));
    return Number.isNaN(epoch.getTime()) ? null : epoch;
  }
  const s = String(value).trim();
  let m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if(m) return new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
  m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if(m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  m = s.match(/^(\d{2})-(\d{2})-(\d{4})/);
  if(m) return new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}
function moneyNumber(v){
  if(v == null || v === '') return 0;
  if(typeof v === 'number') return Number.isFinite(v) ? v : 0;
  let s = String(v).trim().replace(/\s+/g,'').replace(/R\$/g,'');
  if(!s) return 0;
  const hasComma = s.includes(',');
  const hasDot = s.includes('.');
  if(hasComma && hasDot){
    if(s.lastIndexOf(',') > s.lastIndexOf('.')) s = s.replace(/\./g,'').replace(',', '.');
    else s = s.replace(/,/g,'');
  } else if(hasComma){
    s = s.replace(/\./g,'').replace(',', '.');
  } else {
    s = s.replace(/,/g,'');
  }
  s = s.replace(/[^0-9.-]/g,'');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}
function normalizeStr(v){ return String(v || '').trim(); }
function hasFinanceSchema(row){
  if(!row || typeof row !== 'object') return false;
  const keys = Object.keys(row);
  const hasDate = ['Data da venda','Data','Data da Venda'].some(k => keys.includes(k));
  const hasStore = ['Código da loja','Codigo da loja','Cód. loja','Nome da loja','Loja'].some(k => keys.includes(k));
  const hasMoney = ['Itens','Entrega','Desconto','Total'].some(k => keys.includes(k));
  return hasDate && hasStore && hasMoney;
}
function normalizeStore(store='', code=''){
  const cod = String(code || '').trim();
  if(LOJA_CODIGO_MAP[cod]) return LOJA_CODIGO_MAP[cod];
  const s = String(store || '').toLowerCase().trim();
  if(!s) return 'Sem loja informada';
  if(/(^|\s)abc(\s|$)/.test(s) || s.includes('santo andré') || s.includes('santo andre') || s.includes('sao bernardo') || s.includes('são bernardo') || s.includes('são caetano')) return 'Burger ABC';
  if(s.includes('gru') || s.includes('guarulhos')) return 'Burger GRU';
  if(s.includes('zona leste') || s.includes('matriz')) return 'Burger Matriz';
  if(s.includes('gourmet')) return 'Gourmet';
  if(s.includes('forneria') || s.includes('esfiha')) return 'Aham Forneria';
  if(s.includes('sushi')) return 'Aham Sushi';
  if(s.includes('just burger') || s === 'burger' || s.includes('burger')) return 'Burger Matriz';
  return String(store || '').trim() || 'Sem loja informada';
}
function normalizePackedRow(row, fallbackStore=''){
  const data = parseDateFlex(row?.d || row?.data);
  if(!data) return null;
  const loja = normalizeStore(row?.l || row?.loja || fallbackStore || '');
  if(loja === 'Sem loja informada') return null;
  return {
    pedido: String(row?.p ?? row?.pedido ?? ''),
    data,
    loja,
    itens: Number(moneyNumber(row?.i ?? row?.itens).toFixed(2)),
    entrega: Number(moneyNumber(row?.e ?? row?.entrega).toFixed(2)),
    desconto: Number(moneyNumber(row?.dc ?? row?.desconto).toFixed(2)),
    acrescimo: Number(moneyNumber(row?.a ?? row?.acrescimo).toFixed(2)),
    total: Number(moneyNumber(row?.t ?? row?.total).toFixed(2)),
    cancelado: Boolean(row?.c ?? row?.cancelado)
  };
}
function normalizeRowShape(row){
  return {
    p: row.pedido || '',
    d: ymd(row.data),
    l: row.loja,
    i: Number((row.itens || 0).toFixed(2)),
    e: Number((row.entrega || 0).toFixed(2)),
    dc: Number((row.desconto || 0).toFixed(2)),
    a: Number((row.acrescimo || 0).toFixed(2)),
    t: Number((row.total || 0).toFixed(2)),
    c: row.cancelado ? 1 : 0
  };
}
function summarizeRows(rows){
  const byStore = new Map();
  const byDay = new Map();
  for(const row of rows){
    const dayKey = ymd(row.data);
    const s = byStore.get(row.loja) || { loja: row.loja, pedidos: 0, faturamento: 0, desconto: 0, entrega: 0 };
    s.pedidos += row.cancelado ? 0 : 1;
    s.faturamento += row.cancelado ? 0 : row.total;
    s.desconto += row.cancelado ? 0 : row.desconto;
    s.entrega += row.cancelado ? 0 : row.entrega;
    byStore.set(row.loja, s);

    const d = byDay.get(dayKey) || { data: dayKey, pedidos: 0, faturamento: 0, desconto: 0, entrega: 0 };
    d.pedidos += row.cancelado ? 0 : 1;
    d.faturamento += row.cancelado ? 0 : row.total;
    d.desconto += row.cancelado ? 0 : row.desconto;
    d.entrega += row.cancelado ? 0 : row.entrega;
    byDay.set(dayKey, d);
  }
  const stores = STORE_ORDER.map(loja => byStore.get(loja) || { loja, pedidos: 0, faturamento: 0, desconto: 0, entrega: 0 });
  const totals = {
    pedidos: stores.reduce((s,r)=>s+r.pedidos,0),
    faturamento: Number(stores.reduce((s,r)=>s+r.faturamento,0).toFixed(2)),
    desconto: Number(stores.reduce((s,r)=>s+r.desconto,0).toFixed(2)),
    entrega: Number(stores.reduce((s,r)=>s+r.entrega,0).toFixed(2))
  };
  totals.ticket = totals.pedidos ? Number((totals.faturamento / totals.pedidos).toFixed(2)) : 0;
  return { by_store: stores, by_day: [...byDay.values()].sort((a,b)=>a.data.localeCompare(b.data)), totals };
}
async function fetchJson(url){
  const res = await fetch(url, { cache:'no-store' });
  if(!res.ok) throw new Error(`Falha ao ler ${url}: ${res.status}`);
  return res.json();
}
async function writeJson(target, payload){
  await fs.mkdir(path.dirname(target), { recursive: true });
  await fs.writeFile(target, JSON.stringify(payload, null, 2), 'utf-8');
}
function dedupeRows(rows){
  const map = new Map();
  for(const row of rows){
    map.set(`${row.pedido || ''}||${row.loja}||${ymd(row.data)}`, row);
  }
  return [...map.values()].sort((a,b)=>a.data-b.data || a.loja.localeCompare(b.loja));
}

async function loadIndexedRows(){
  const companiesMeta = await fetchJson(INDEXED_META_COMPANIES_URL).catch(() => null);
  if(!companiesMeta || typeof companiesMeta !== 'object') return null;
  const rows = [];
  for(const [slug, companyMeta] of Object.entries(companiesMeta)){
    const companyNode = await fetchJson(INDEXED_COMPANY_URL(slug)).catch(() => null);
    if(!companyNode || typeof companyNode !== 'object') continue;
    const displayName = normalizeStore(companyNode?.display_name || companyNode?.source_name || companyMeta?.display_name || companyMeta?.source_name || slug);
    const months = companyNode?.months || {};
    for(const monthNode of Object.values(months)){
      const packedRows = Object.values(monthNode?.rows || {});
      for(const packed of packedRows){
        const row = normalizePackedRow(packed, displayName);
        if(row) rows.push(row);
      }
    }
  }
  return rows.length ? dedupeRows(rows) : null;
}

function getReportRefDate(rep){
  return parseDateFlex(rep?.data_referencia) || (rep?.timestamp_upload ? new Date(rep.timestamp_upload) : null);
}
async function fetchReportRows(rep){
  let parsed = {};
  try { parsed = JSON.parse(rep.dados_json || '{}'); } catch { parsed = {}; }
  let rows = Array.isArray(parsed.linhas) ? parsed.linhas.slice() : [];
  if(rep.chunked && rep.chunks_path){
    try {
      const chunkObj = await fetchJson(`${FIREBASE_URL}/${rep.chunks_path}.json`);
      const chunkList = Array.isArray(chunkObj) ? chunkObj.filter(Boolean) : Object.values(chunkObj || {});
      chunkList.sort((a,b)=>(a?.chunk_index||0)-(b?.chunk_index||0));
      rows = chunkList.flatMap(c => Array.isArray(c?.rows) ? c.rows : []);
    } catch (err) {
      console.error('Erro lendo chunks', rep.nome_arquivo || rep.id, err.message);
    }
  }
  return rows;
}
async function mapWithConcurrency(list, limit, mapper){
  const safeLimit = Math.max(1, Math.min(limit || 4, 8));
  const results = new Array(list.length);
  let cursor = 0;
  async function worker(){
    while(cursor < list.length){
      const index = cursor++;
      results[index] = await mapper(list[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(safeLimit, list.length) }, () => worker()));
  return results;
}
async function loadLegacyRows(){
  const reportsObj = await fetchJson(LEGACY_REPORTS_URL) || {};
  const reports = Object.entries(reportsObj)
    .map(([id, value]) => ({ id, ...value }))
    .filter(r => r.status !== 'deletado' && r.tipo === 'financeiro')
    .sort((a,b)=>(b.timestamp_upload||0)-(a.timestamp_upload||0));

  const reportRows = await mapWithConcurrency(reports, 6, async rep => ({ rep, rows: await fetchReportRows(rep) }));
  const dedupe = new Map();

  for(const { rep, rows } of reportRows){
    for(const [idx, row] of (rows || []).entries()){
      if(!hasFinanceSchema(row)) continue;
      const data = parseDateFlex(row['Data da venda'] || row['Data'] || row['Data da Venda'] || rep.data_referencia);
      if(!data) continue;
      const code = row['Código da loja'] || row['Codigo da loja'] || row['Cód. loja'] || '';
      const lojaOriginal = normalizeStr(row['Nome da loja'] || row['Loja'] || rep.loja_arquivo || rep.chave_loja_arquivo || rep.nome_arquivo || '');
      const loja = normalizeStore(lojaOriginal, code);
      if(loja === 'Sem loja informada') continue;
      const pedidoKey = row['Id do pedido no parceiro'] || row['Número do pedido no parceiro'] || row['Pedido'] || `${rep.id}_${idx}`;
      dedupe.set(`${pedidoKey}||${loja}||${ymd(data)}`, {
        pedido: String(pedidoKey),
        data,
        loja,
        itens: moneyNumber(row['Itens']),
        entrega: moneyNumber(row['Entrega']),
        desconto: moneyNumber(row['Desconto']),
        acrescimo: moneyNumber(row['Acréscimo'] || row['Acrescimo']),
        total: moneyNumber(row['Total']),
        cancelado: /^S$/i.test(normalizeStr(row['Está cancelado'] || row['Esta cancelado'] || row['Cancelado']))
      });
    }
  }
  return dedupeRows([...dedupe.values()]);
}

async function main(){
  const rows = await loadIndexedRows().catch(() => null) || await loadLegacyRows();
  const monthsAvailable = [...new Set(rows.map(r => monthKey(r.data)))].sort();
  const latestMonth = monthsAvailable[monthsAvailable.length - 1] || null;
  const previousMonth = latestMonth ? (() => { const d = monthStart(latestMonth); d.setMonth(d.getMonth()-1); return monthKey(d); })() : null;

  const root = path.resolve(process.cwd(), OUTPUT_ROOT);
  const filesAllMonths = {};
  const filesByStore = {};

  for(const mk of monthsAvailable){
    const monthRows = rows.filter(r => monthKey(r.data) === mk);
    const pack = {
      version: 2,
      month_key: mk,
      scope: 'all',
      row_count: monthRows.length,
      period_start: monthRows.length ? ymd(monthRows[0].data) : `${mk}-01`,
      period_end: monthRows.length ? ymd(monthRows[monthRows.length - 1].data) : `${mk}-31`,
      generated_at: new Date().toISOString(),
      aggregates: summarizeRows(monthRows),
      rows: monthRows.map(normalizeRowShape)
    };
    const rel = `${OUTPUT_ROOT}/months/${mk}.json`;
    filesAllMonths[mk] = rel;
    await writeJson(path.join(root, 'months', `${mk}.json`), pack);

    for(const store of STORE_ORDER){
      const storeRows = monthRows.filter(r => r.loja === store);
      if(!storeRows.length) continue;
      const slug = DEFAULT_STORE_SLUGS[store];
      const relStore = `${OUTPUT_ROOT}/stores/${slug}/${mk}.json`;
      filesByStore[slug] ||= {};
      filesByStore[slug][mk] = relStore;
      await writeJson(path.join(root, 'stores', slug, `${mk}.json`), {
        version: 2,
        month_key: mk,
        scope: 'store',
        store,
        row_count: storeRows.length,
        period_start: ymd(storeRows[0].data),
        period_end: ymd(storeRows[storeRows.length - 1].data),
        generated_at: new Date().toISOString(),
        aggregates: summarizeRows(storeRows),
        rows: storeRows.map(normalizeRowShape)
      });
    }
  }

  if(latestMonth && filesAllMonths[latestMonth]){
    const currentPayload = JSON.parse(await fs.readFile(path.join(root, 'months', `${latestMonth}.json`), 'utf-8'));
    await writeJson(path.join(root, 'current', 'current-all.json'), currentPayload);
    await writeJson(path.resolve(process.cwd(), 'vendas-cache-mes-atual.json'), {
      generated_at: currentPayload.generated_at,
      month_key: currentPayload.month_key,
      period_start: currentPayload.period_start,
      period_end: currentPayload.period_end,
      source: 'data-vendas-index/current',
      row_count: currentPayload.row_count,
      stores_covered: STORE_ORDER.filter(name => currentPayload.aggregates?.by_store?.some(item => item.loja === name && item.pedidos > 0)),
      rows: currentPayload.rows
    });
  }
  if(previousMonth && filesAllMonths[previousMonth]){
    const prevPayload = JSON.parse(await fs.readFile(path.join(root, 'months', `${previousMonth}.json`), 'utf-8'));
    await writeJson(path.join(root, 'current', 'previous-all.json'), prevPayload);
  } else {
    await writeJson(path.join(root, 'current', 'previous-all.json'), { version: 2, month_key: previousMonth, scope: 'all', row_count: 0, generated_at: new Date().toISOString(), aggregates: { by_store: [], by_day: [], totals: { pedidos:0, faturamento:0, desconto:0, entrega:0, ticket:0 } }, rows: [] });
  }

  await writeJson(path.join(root, 'meta.json'), {
    version: 2,
    generated_at: new Date().toISOString(),
    source: 'indexed-firebase-or-legacy-reports',
    row_count: rows.length,
    stores: STORE_ORDER.map(name => ({ name, slug: DEFAULT_STORE_SLUGS[name] })),
    months_available: monthsAvailable,
    latest_month: latestMonth,
    previous_month: previousMonth,
    files: {
      all_months: filesAllMonths,
      by_store: filesByStore,
      current: {
        current_all: `${OUTPUT_ROOT}/current/current-all.json`,
        previous_all: `${OUTPUT_ROOT}/current/previous-all.json`
      }
    }
  });

  console.log(`Índices gerados em: ${root}`);
  console.log(`Linhas únicas: ${rows.length}`);
  console.log(`Meses disponíveis: ${monthsAvailable.join(', ') || 'nenhum'}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
