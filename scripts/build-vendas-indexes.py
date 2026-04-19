#!/usr/bin/env python3
import json
import math
import sqlite3
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

FIREBASE_URL = 'https://justburger-producao-e9b27-default-rtdb.firebaseio.com'
REPORTS_URL = f'{FIREBASE_URL}/relatorios_diarios.json'
OUTPUT_ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('data/vendas-index')
PACKAGE_ROOT = OUTPUT_ROOT.parent.parent if OUTPUT_ROOT.parts[-2:] == ('data','vendas-index') else Path('.')
STORE_ORDER = ['Burger Matriz','Burger ABC','Burger GRU','Gourmet','Aham Forneria','Aham Sushi']
LOJA_CODIGO_MAP = {
    '21870':'Burger Matriz',
    'aa6576e8-574e-42c5-8d38-0d7df31f3fcc':'Burger ABC',
    '8432b620-27ea-48fb-bf3b-a0b76bbf3516':'Burger GRU',
    '510cb9b9-a4dc-4962-a70f-cc01b0e4bdc8':'Aham Forneria',
    '1eb0ccfe-ba7e-43ad-9a23-06c4ea7a823a':'Aham Forneria',
    'fe4e06bd-2f30-410a-97ef-b21eba59f3e4':'Aham Sushi',
    '20215':'Aham Sushi'
}

def fetch_json(url: str):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=300) as resp:
        return json.loads(resp.read().decode('utf-8'))

def slugify(v=''):
    import unicodedata, re
    s = unicodedata.normalize('NFD', str(v or ''))
    s = ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '-', s).strip('-')
    return s

def parse_date_flex(value):
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        epoch = datetime(1899, 12, 30) + timedelta(days=float(value))
        return epoch
    s = str(value).strip()
    for fmt in ['%d/%m/%Y %H:%M', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None

def money_number(v):
    if v is None or v == '':
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(' ', '').replace('R$', '')
    if not s:
        return 0.0
    has_comma = ',' in s
    has_dot = '.' in s
    if has_comma and has_dot:
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    elif has_comma:
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '')
    filtered = ''.join(ch for ch in s if ch in '0123456789.-')
    try:
        return float(filtered)
    except Exception:
        return 0.0

def normalize_str(v):
    return str(v or '').strip()

def has_finance_schema(row):
    if not isinstance(row, dict):
        return False
    keys = set(row.keys())
    has_date = any(k in keys for k in ['Data da venda', 'Data', 'Data da Venda'])
    has_store = any(k in keys for k in ['Código da loja', 'Codigo da loja', 'Cód. loja', 'Nome da loja', 'Loja'])
    has_money = any(k in keys for k in ['Itens', 'Entrega', 'Desconto', 'Total'])
    return has_date and has_store and has_money

def normalize_store(store='', code=''):
    cod = str(code or '').strip()
    if cod in LOJA_CODIGO_MAP:
        return LOJA_CODIGO_MAP[cod]
    s = str(store or '').lower().strip()
    if not s:
        return 'Sem loja informada'
    if (' abc ' in f' {s} ') or ('santo andré' in s) or ('santo andre' in s) or ('sao bernardo' in s) or ('são bernardo' in s) or ('são caetano' in s):
        return 'Burger ABC'
    if 'gru' in s or 'guarulhos' in s:
        return 'Burger GRU'
    if 'zona leste' in s or 'matriz' in s:
        return 'Burger Matriz'
    if 'gourmet' in s:
        return 'Gourmet'
    if 'forneria' in s or 'esfiha' in s:
        return 'Aham Forneria'
    if 'sushi' in s:
        return 'Aham Sushi'
    if 'just burger' in s or s == 'burger' or 'burger' in s:
        return 'Burger Matriz'
    return str(store or '').strip() or 'Sem loja informada'

def report_store_hint(rep):
    return normalize_str(rep.get('loja_arquivo') or rep.get('chave_loja_arquivo') or rep.get('nome_arquivo') or '')

def fetch_report_rows(rep):
    parsed = {}
    try:
        parsed = json.loads(rep.get('dados_json') or '{}')
    except Exception:
        parsed = {}
    rows = list(parsed.get('linhas') or []) if isinstance(parsed, dict) else []
    chunks_path = rep.get('chunks_path')
    if rep.get('chunked') and chunks_path:
        chunk_obj = fetch_json(f"{FIREBASE_URL}/{str(chunks_path).strip('/')}.json") or {}
        if isinstance(chunk_obj, list):
            chunk_list = [x for x in chunk_obj if x]
        else:
            chunk_list = list((chunk_obj or {}).values())
        chunk_list.sort(key=lambda x: (x or {}).get('chunk_index', 0))
        rows = []
        for chunk in chunk_list:
            rows.extend((chunk or {}).get('rows') or [])
    return rows

def write_json(target: Path, payload):
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')

def summarize_sql(conn, month_key=None, store=None):
    params = []
    where = ['cancelado=0']
    if month_key is not None:
        where.append('month_key=?')
        params.append(month_key)
    if store is not None:
        where.append('loja=?')
        params.append(store)
    where_sql = ' where ' + ' and '.join(where)
    by_store = {name:{'loja':name,'pedidos':0,'faturamento':0.0,'desconto':0.0,'entrega':0.0} for name in STORE_ORDER}
    cur = conn.execute(f'''select loja, count(*) as pedidos, round(sum(total),2), round(sum(desconto),2), round(sum(entrega),2)
                           from rows {where_sql} group by loja order by loja''', params)
    for loja, pedidos, faturamento, desconto, entrega in cur.fetchall():
        by_store[loja] = {
            'loja': loja,
            'pedidos': int(pedidos or 0),
            'faturamento': float(faturamento or 0),
            'desconto': float(desconto or 0),
            'entrega': float(entrega or 0),
        }
    stores = [by_store[name] for name in STORE_ORDER]
    cur = conn.execute(f'''select data, count(*) as pedidos, round(sum(total),2), round(sum(desconto),2), round(sum(entrega),2)
                           from rows {where_sql} group by data order by data''', params)
    by_day = [
        {'data': data, 'pedidos': int(p or 0), 'faturamento': float(f or 0), 'desconto': float(d or 0), 'entrega': float(e or 0)}
        for data, p, f, d, e in cur.fetchall()
    ]
    totals = {
        'pedidos': sum(x['pedidos'] for x in stores),
        'faturamento': round(sum(x['faturamento'] for x in stores), 2),
        'desconto': round(sum(x['desconto'] for x in stores), 2),
        'entrega': round(sum(x['entrega'] for x in stores), 2),
    }
    totals['ticket'] = round(totals['faturamento'] / totals['pedidos'], 2) if totals['pedidos'] else 0.0
    return {'by_store': stores, 'by_day': by_day, 'totals': totals}

def export_rows(conn, month_key, store=None):
    params = [month_key]
    sql = 'select pedido, data, loja, itens, entrega, desconto, acrescimo, total, cancelado from rows where month_key=?'
    if store is not None:
        sql += ' and loja=?'
        params.append(store)
    sql += ' order by data, loja, pedido'
    cur = conn.execute(sql, params)
    rows = []
    for pedido, data, loja, itens, entrega, desconto, acrescimo, total, cancelado in cur.fetchall():
        rows.append({
            'p': pedido,
            'd': data,
            'l': loja,
            'i': round(float(itens or 0), 2),
            'e': round(float(entrega or 0), 2),
            'dc': round(float(desconto or 0), 2),
            'a': round(float(acrescimo or 0), 2),
            't': round(float(total or 0), 2),
            'c': 1 if cancelado else 0,
        })
    return rows

def main():
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    tmp_dir = OUTPUT_ROOT.parent / '.tmp'
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = tmp_dir / 'vendas-index-build.sqlite'
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(str(db_path))
    conn.execute('pragma journal_mode=wal')
    conn.execute('pragma synchronous=normal')
    conn.execute('pragma temp_store=memory')
    conn.execute('''create table rows (
        dedupe_key text primary key,
        pedido text,
        data text,
        month_key text,
        loja text,
        itens real,
        entrega real,
        desconto real,
        acrescimo real,
        total real,
        cancelado integer
    )''')
    conn.execute('create index idx_rows_month on rows(month_key)')
    conn.execute('create index idx_rows_month_store on rows(month_key, loja)')
    conn.execute('create index idx_rows_data on rows(data)')

    reports_obj = fetch_json(REPORTS_URL) or {}
    reports = [dict({'id': rid}, **value) for rid, value in reports_obj.items() if isinstance(value, dict)]
    reports = [r for r in reports if r.get('status') != 'deletado' and r.get('tipo') == 'financeiro']
    reports.sort(key=lambda r: r.get('timestamp_upload') or 0)

    inserted = 0
    for idx, rep in enumerate(reports, 1):
        rows = fetch_report_rows(rep)
        batch = []
        for row_idx, row in enumerate(rows):
            if not has_finance_schema(row):
                continue
            data = parse_date_flex(row.get('Data da venda') or row.get('Data') or row.get('Data da Venda') or rep.get('data_referencia'))
            if not data:
                continue
            code = row.get('Código da loja') or row.get('Codigo da loja') or row.get('Cód. loja') or ''
            loja_original = normalize_str(row.get('Nome da loja') or row.get('Loja') or report_store_hint(rep))
            loja = normalize_store(loja_original, code)
            if loja == 'Sem loja informada':
                continue
            pedido_key = row.get('Id do pedido no parceiro') or row.get('Número do pedido no parceiro') or row.get('Pedido') or f"{rep['id']}_{row_idx}"
            data_ymd = f'{data.year:04d}-{data.month:02d}-{data.day:02d}'
            month_key = f'{data.year:04d}-{data.month:02d}'
            dedupe_key = f"{pedido_key}||{loja}||{data_ymd}"
            batch.append((
                dedupe_key,
                str(pedido_key),
                data_ymd,
                month_key,
                loja,
                money_number(row.get('Itens')),
                money_number(row.get('Entrega')),
                money_number(row.get('Desconto')),
                money_number(row.get('Acréscimo') or row.get('Acrescimo')),
                money_number(row.get('Total')),
                1 if str(row.get('Está cancelado') or row.get('Esta cancelado') or row.get('Cancelado') or '').strip().upper() == 'S' else 0,
            ))
            if len(batch) >= 5000:
                conn.executemany('insert or replace into rows values (?,?,?,?,?,?,?,?,?,?,?)', batch)
                inserted += len(batch)
                batch = []
        if batch:
            conn.executemany('insert or replace into rows values (?,?,?,?,?,?,?,?,?,?,?)', batch)
            inserted += len(batch)
        conn.commit()
        print(f'Processado {idx}/{len(reports)}: {rep.get("nome_arquivo") or rep["id"]}')

    months_available = [row[0] for row in conn.execute('select distinct month_key from rows order by month_key').fetchall()]
    latest_month = months_available[-1] if months_available else None
    previous_month = None
    if latest_month:
        dt = datetime.strptime(latest_month + '-01', '%Y-%m-%d')
        if dt.month == 1:
            previous_month = f'{dt.year - 1}-12'
        else:
            previous_month = f'{dt.year:04d}-{dt.month - 1:02d}'

    files_all_months = {}
    files_by_store = defaultdict(dict)
    store_slugs = {name: slugify(name) for name in STORE_ORDER}
    generated_at = datetime.utcnow().isoformat() + 'Z'

    for mk in months_available:
        row_count = conn.execute('select count(*) from rows where month_key=?', (mk,)).fetchone()[0]
        period_start = conn.execute('select min(data) from rows where month_key=?', (mk,)).fetchone()[0] or f'{mk}-01'
        period_end = conn.execute('select max(data) from rows where month_key=?', (mk,)).fetchone()[0] or f'{mk}-31'
        all_payload = {
            'version': 4,
            'month_key': mk,
            'scope': 'all',
            'row_count': row_count,
            'period_start': period_start,
            'period_end': period_end,
            'generated_at': generated_at,
            'aggregates': summarize_sql(conn, month_key=mk),
            'rows': export_rows(conn, mk),
        }
        files_all_months[mk] = f'data/vendas-index/months/{mk}.json'
        write_json(OUTPUT_ROOT / 'months' / f'{mk}.json', all_payload)

        for store in STORE_ORDER:
            store_count = conn.execute('select count(*) from rows where month_key=? and loja=?', (mk, store)).fetchone()[0]
            if not store_count:
                continue
            slug = store_slugs[store]
            files_by_store[slug][mk] = f'data/vendas-index/stores/{slug}/{mk}.json'
            store_payload = {
                'version': 4,
                'month_key': mk,
                'scope': 'store',
                'store': store,
                'row_count': store_count,
                'period_start': conn.execute('select min(data) from rows where month_key=? and loja=?', (mk, store)).fetchone()[0],
                'period_end': conn.execute('select max(data) from rows where month_key=? and loja=?', (mk, store)).fetchone()[0],
                'generated_at': generated_at,
                'aggregates': summarize_sql(conn, month_key=mk, store=store),
                'rows': export_rows(conn, mk, store=store),
            }
            write_json(OUTPUT_ROOT / 'stores' / slug / f'{mk}.json', store_payload)

    if latest_month:
        current_payload = json.loads((OUTPUT_ROOT / 'months' / f'{latest_month}.json').read_text(encoding='utf-8'))
        write_json(OUTPUT_ROOT / 'current' / 'current-all.json', current_payload)
        compat_payload = {
            'generated_at': current_payload['generated_at'],
            'month_key': current_payload['month_key'],
            'period_start': current_payload['period_start'],
            'period_end': current_payload['period_end'],
            'source': 'data-vendas-index/current',
            'row_count': current_payload['row_count'],
            'stores_covered': [x['loja'] for x in current_payload['aggregates']['by_store'] if x['pedidos'] > 0],
            'rows': current_payload['rows'],
        }
        write_json(PACKAGE_ROOT / 'vendas-cache-mes-atual.json', compat_payload)
    if previous_month and (OUTPUT_ROOT / 'months' / f'{previous_month}.json').exists():
        prev_payload = json.loads((OUTPUT_ROOT / 'months' / f'{previous_month}.json').read_text(encoding='utf-8'))
    else:
        prev_payload = {
            'version': 4,
            'month_key': previous_month,
            'scope': 'all',
            'row_count': 0,
            'generated_at': generated_at,
            'aggregates': {'by_store': [], 'by_day': [], 'totals': {'pedidos': 0, 'faturamento': 0, 'desconto': 0, 'entrega': 0, 'ticket': 0}},
            'rows': [],
        }
    write_json(OUTPUT_ROOT / 'current' / 'previous-all.json', prev_payload)

    row_count = conn.execute('select count(*) from rows').fetchone()[0]
    meta = {
        'version': 4,
        'generated_at': generated_at,
        'source': 'firebase-relatorios_diarios',
        'report_count': len(reports),
        'row_count': row_count,
        'stores': [{'name': name, 'slug': store_slugs[name]} for name in STORE_ORDER],
        'months_available': months_available,
        'latest_month': latest_month,
        'previous_month': previous_month,
        'files': {
            'all_months': files_all_months,
            'by_store': files_by_store,
            'current': {
                'current_all': 'data/vendas-index/current/current-all.json',
                'previous_all': 'data/vendas-index/current/previous-all.json'
            }
        }
    }
    write_json(OUTPUT_ROOT / 'meta.json', meta)

    print(f'Índices gerados em: {OUTPUT_ROOT.resolve()}')
    print(f'Relatórios processados: {len(reports)}')
    print(f'Linhas inseridas (com reposição): {inserted}')
    print(f'Linhas únicas: {row_count}')
    print(f'Meses disponíveis: {", ".join(months_available)}')

if __name__ == '__main__':
    main()
