// POST /api/sync  — 기기 간 단어장 동기화 (Upstash Redis에 저장)
const crypto = require('crypto');
const { Redis } = require('@upstash/redis');

const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const redis = url && token ? new Redis({ url, token }) : null;

const KEY = 'vocab:items';
const MAX_ITEMS = 5000;
const TOMBSTONE_MS = 90 * 24 * 60 * 60 * 1000; // 삭제 기록은 90일 후 정리

const same = (a, b) => {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};
const sleep = ms => new Promise(r => setTimeout(r, ms));

function clean(it) {
  if (!it || typeof it.id !== 'string' || typeof it.w !== 'string') return null;
  const now = Date.now();
  const num = (v, d) => (Number.isFinite(Number(v)) ? Number(v) : d);
  return {
    id: it.id.slice(0, 64),
    w: it.w.slice(0, 100),
    d: /^\d{4}-\d{2}-\d{2}$/.test(it.d) ? it.d : new Date().toISOString().slice(0, 10),
    done: !!it.done,
    star: !!it.star,
    meaning: typeof it.meaning === 'string' ? it.meaning.slice(0, 500) : '',
    deleted: !!it.deleted,
    c: num(it.c, now),
    u: num(it.u, now)
  };
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'POST') return res.status(405).json({ error: 'method_not_allowed' });

  const password = process.env.VOCAB_PASSWORD;
  if (!password || !redis) return res.status(500).json({ error: 'not_configured' });

  if (!same(req.headers['x-vocab-password'] || '', password)) {
    await sleep(600); // 무작위 대입 공격 속도 늦추기
    return res.status(401).json({ error: 'unauthorized' });
  }

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {};
    const incoming = Array.isArray(body.items) ? body.items.slice(0, MAX_ITEMS * 2) : [];

    let stored = await redis.get(KEY);
    if (typeof stored === 'string') { try { stored = JSON.parse(stored); } catch (e) { stored = []; } }
    if (!Array.isArray(stored)) stored = [];

    // 항목 id별로 가장 최근에 수정된(u가 큰) 것을 남겨요
    const map = new Map();
    for (const raw of stored.concat(incoming)) {
      const it = clean(raw);
      if (!it) continue;
      const cur = map.get(it.id);
      if (!cur || it.u >= cur.u) map.set(it.id, it);
    }
    const cutoff = Date.now() - TOMBSTONE_MS;
    const merged = Array.from(map.values())
      .filter(x => !(x.deleted && x.u < cutoff))
      .sort((a, b) => b.c - a.c)
      .slice(0, MAX_ITEMS);

    await redis.set(KEY, merged);
    return res.status(200).json({ items: merged });
  } catch (e) {
    return res.status(500).json({ error: 'server_error' });
  }
};
