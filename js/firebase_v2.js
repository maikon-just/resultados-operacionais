/* ═══════════════════════════════════════════════════════
   JUST BURGER 🍔 — firebase_v2.js
   Firebase Realtime Database — SDK compat (sem módulos ES)

   ✅ COMO CONFIGURAR:
   1. Acesse https://console.firebase.google.com
   2. Selecione seu projeto (ou crie um novo)
   3. Clique em "Configurações do projeto" (⚙️)
   4. Em "Seus apps", clique em "</>" para registrar um web app
   5. Copie o objeto firebaseConfig e cole no bloco FIREBASE_CONFIG abaixo
   6. No menu lateral, acesse "Realtime Database" e crie o banco
   7. Configure as Regras (Rules) para desenvolvimento:
      { "rules": { ".read": true, ".write": true } }
═══════════════════════════════════════════════════════════ */

/* ── 🔧 SUBSTITUA PELAS SUAS CREDENCIAIS DO FIREBASE ─────
   Acesse: console.firebase.google.com → Seu projeto
   → Configurações → Seus apps → SDK de configuração → Config  */
const FIREBASE_CONFIG = {
  apiKey:            "AIzaSyDt1yzxGGzuT2cSdten1bFuXG7yeDQ90Po",
  authDomain:        "justburger-producao-e9b27.firebaseapp.com",
  databaseURL:       "https://justburger-producao-e9b27-default-rtdb.firebaseio.com",
  projectId:         "justburger-producao-e9b27",
  storageBucket:     "justburger-producao-e9b27.firebasestorage.app",
  messagingSenderId: "387070713910",
  appId:             "1:387070713910:web:f156d3247a1715a58ab897"
};

/* ── Init ─────────────────────────────────────────────── */
if (!firebase.apps.length) {
  firebase.initializeApp(FIREBASE_CONFIG);
}
const fbDB_v2 = firebase.database();
window.fbDB_v2 = fbDB_v2;

/* ── Helpers internos ─────────────────────────────────── */
function _snapToList(snap) {
  const val = snap.val();
  if (!val) return [];
  return Object.entries(val).map(([id, obj]) => ({ id, ...obj }));
}
function _ts() { return Date.now(); }

/* ══════════════════════════════════════════════════════
   CRUD GENÉRICO
══════════════════════════════════════════════════════ */

/** GET todos os registros de uma coleção → retorna array */
function fbv2_getAll(col) {
  return fbDB_v2.ref(col).once('value').then(snap => _snapToList(snap));
}

/** GET um registro por ID */
function fbv2_getOne(col, id) {
  return fbDB_v2.ref(col + '/' + id).once('value').then(snap => {
    if (!snap.exists()) return null;
    return { id: snap.key, ...snap.val() };
  });
}

/** POST — cria novo registro com ID automático */
function fbv2_post(col, data) {
  const ref = fbDB_v2.ref(col).push();
  const obj = { ...data, created_at: _ts(), updated_at: _ts() };
  return ref.set(obj).then(() => ({ id: ref.key, ...obj }));
}

/** PATCH — atualiza campos parcialmente */
function fbv2_patch(col, id, data) {
  const obj = { ...data, updated_at: _ts() };
  return fbDB_v2.ref(col + '/' + id).update(obj).then(() => ({ id, ...obj }));
}

/** PUT — substitui o registro inteiro */
function fbv2_put(col, id, data) {
  const obj = { ...data, updated_at: _ts() };
  return fbDB_v2.ref(col + '/' + id).set(obj).then(() => ({ id, ...obj }));
}

/** DELETE — remove o registro */
function fbv2_delete(col, id) {
  return fbDB_v2.ref(col + '/' + id).remove();
}

/* ── Expõe globalmente ─────────────────────────────── */
window.FB2 = {
  getAll:  fbv2_getAll,
  getOne:  fbv2_getOne,
  post:    fbv2_post,
  patch:   fbv2_patch,
  put:     fbv2_put,
  delete:  fbv2_delete,
};

/* Aliases globais para compatibilidade com app_v3.js */
window.fbv2_getAll   = fbv2_getAll;
window.fbv2_getOne   = fbv2_getOne;
window.fbv2_post     = fbv2_post;
window.fbv2_patch    = fbv2_patch;
window.fbv2_put      = fbv2_put;
window.fbv2_delete   = fbv2_delete;

console.log('🔥 Just Burger — Firebase Realtime DB conectado!');
