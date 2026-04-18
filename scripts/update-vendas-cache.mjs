import fs from 'node:fs/promises';
import path from 'node:path';

const FIREBASE_URL = 'https://justburger-producao-e9b27-default-rtdb.firebaseio.com';
const REPORTS_URL = `${FIREBASE_URL}/relatorios_diarios.json`;
const OUTPUT_FILE = process.argv[2] || 'vendas-cache-mes-atual.json';
const STORE_ORDER = ['Burger Matriz','Burger ABC','Burger GRU','Gourmet','Aham Forneria','Aham Sushi'];
const LOJA_CODIGO_MAP = {
  '21870':'Burger Matriz',
  'aa6576e8-574e-42c5-8d38-0d7df31f3fcc':'Burger ABC',
  '8432b620-27ea-48fb-bf3b-a0b76bbf3516':'Burger GRU',
  '510cb9b9-a4dc-4962-a70f-cc01b0e4bdc8':'Aham Forneria',
  '1eb0ccfe-ba7e-43ad-9a23-06c4ea7a823a':'Aham Forneria',
  'fe4e06bd-2f30-410a-97ef-b21eba59f3e4':'Aham Sushi',
  '20215':'Aham Sushi'
};

function saoPauloNow(){
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }));
}
function ymd(d){
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
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
function normalizeStr(v){
  return String(v || '').trim();
}
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
function getReportRefDate(rep){
  return parseDateFlex(rep?.data_referencia) || (rep?.timestamp_upload ? new Date(rep.timestamp_upload) : null);
}
async function fetchJson(url){
  const res = await fetch(url, { cache: 'no-store' });
  if(!res.ok) throw new Error(`Falha ao ler ${url}: ${res.status}`);
  return res.json();
}
async function fetchReportRows(rep){
  let parsed = {};
  try { parsed = JSON.parse(rep.dados_json || '{}'); } catch { parsed = {}; }
  let rows = Array.isArray(parsed.linhas) ? parsed.linhas.slice() : [];
  if(rep.chunked && rep.chunks_path){
    try {
      const chunkObj = await fetchJson(`${FIREBASE_URL}/${rep.chunks_path}.json`);
      let chunkList = [];
      if(Array.isArray(chunkObj)) chunkList = chunkObj.filter(Boolean);
      else chunkList = Object.values(chunkObj || {});
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

async function main(){
  const now = saoPauloNow();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0);
  const monthKey = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;

  const reportsObj = await fetchJson(REPORTS_URL) || {};
  const reports = Object.entries(reportsObj)
    .map(([id, value]) => ({ id, ...value }))
    .filter(r => r.status !== 'deletado' && r.tipo === 'financeiro')
    .filter(r => {
      const ref = getReportRefDate(r);
      return !ref || ref >= monthStart;
    })
    .sort((a,b)=>(b.timestamp_upload||0)-(a.timestamp_upload||0));

  const reportRows = await mapWithConcurrency(reports, 6, async rep => ({ rep, rows: await fetchReportRows(rep) }));
  const dedupe = new Map();

  for(const { rep, rows } of reportRows){
    (rows || []).forEach((row, idx) => {
      if(!hasFinanceSchema(row)) return;
      const data = parseDateFlex(row['Data da venda'] || row['Data'] || row['Data da Venda'] || rep.data_referencia);
      if(!data) return;
      if(data < monthStart || data > monthEnd) return;
      const code = row['Código da loja'] || row['Codigo da loja'] || row['Cód. loja'] || '';
      const lojaOriginal = normalizeStr(row['Nome da loja'] || row['Loja'] || rep.loja_arquivo || rep.chave_loja_arquivo || rep.nome_arquivo || '');
      const loja = normalizeStore(lojaOriginal, code);
      if(loja === 'Sem loja informada') return;
      const pedidoKey = row['Id do pedido no parceiro'] || row['Número do pedido no parceiro'] || row['Pedido'] || `${rep.id}_${idx}`;
      const key = `${pedidoKey}||${loja}||${ymd(data)}`;
      dedupe.set(key, {
        p: String(pedidoKey),
        d: ymd(data),
        l: loja,
        i: Number(moneyNumber(row['Itens']).toFixed(2)),
        e: Number(moneyNumber(row['Entrega']).toFixed(2)),
        dc: Number(moneyNumber(row['Desconto']).toFixed(2)),
        a: Number(moneyNumber(row['Acréscimo'] || row['Acrescimo']).toFixed(2)),
        t: Number(moneyNumber(row['Total']).toFixed(2)),
        c: /^S$/i.test(normalizeStr(row['Está cancelado'] || row['Esta cancelado'] || row['Cancelado'])) ? 1 : 0
      });
    });
  }

  const rows = [...dedupe.values()].sort((a,b) => String(a.d).localeCompare(String(b.d)) || String(a.l).localeCompare(String(b.l)));
  const storesCovered = [...new Set(rows.map(r => r.l))].sort((a,b) => {
    const ai = STORE_ORDER.indexOf(a);
    const bi = STORE_ORDER.indexOf(b);
    if(ai === -1 && bi === -1) return a.localeCompare(b);
    if(ai === -1) return 1;
    if(bi === -1) return -1;
    return ai - bi;
  });

  const payload = {
    generated_at: new Date().toISOString(),
    month_key: monthKey,
    period_start: ymd(monthStart),
    period_end: ymd(monthEnd),
    source: 'firebase-relatorios_diarios',
    row_count: rows.length,
    stores_covered: storesCovered,
    rows
  };

  const target = path.resolve(process.cwd(), OUTPUT_FILE);
  await fs.mkdir(path.dirname(target), { recursive: true });
  await fs.writeFile(target, JSON.stringify(payload), 'utf-8');
  console.log(`Arquivo gerado: ${target}`);
  console.log(`Linhas: ${rows.length}`);
  console.log(`Lojas: ${storesCovered.join(', ')}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
