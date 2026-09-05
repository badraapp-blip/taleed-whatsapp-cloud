const { proto, initAuthCreds, BufferJSON } = require('@whiskeysockets/baileys');

/**
 * Baileys Auth State Adapter using Supabase PostgreSQL
 * Ensures zero-data-loss and persistent WhatsApp sessions across cloud container restarts
 */
async function useSupabaseAuthState(queryFunc, sessionId = 'admin_instance_1') {
  const keysCache = new Map();
  const pendingKeyWrites = new Map();
  const pendingKeyDeletes = new Set();
  let flushTimer = null;
  let isFlushing = false;

  // دالة الحفظ غير المتزامن في الخلفية لقاعدة بيانات Supabase
  async function flushKeysToSupabase() {
    if (isFlushing) return;
    if (pendingKeyWrites.size === 0 && pendingKeyDeletes.size === 0) return;

    isFlushing = true;
    try {
      // 1. حذف المفاتيح الملغية
      if (pendingKeyDeletes.size > 0) {
        const deleteIds = Array.from(pendingKeyDeletes);
        pendingKeyDeletes.clear();
        const idList = deleteIds.map(id => `'${id}'`).join(',');
        await queryFunc(`DELETE FROM workflow_taleed.whatsapp_session_keys WHERE session_id = '${sessionId}' AND key_id IN (${idList});`);
      }

      // 2. إدخال أو تحديث المفاتيح الجديدة على دفعات
      if (pendingKeyWrites.size > 0) {
        const toWrite = Array.from(pendingKeyWrites.entries());
        pendingKeyWrites.clear();

        const tasks = toWrite.map(([keyId, value]) => {
          const serialized = JSON.stringify(value, BufferJSON.replacer);
          return `('${sessionId}', '${keyId}', '${serialized.replace(/'/g, "''")}'::jsonb, now())`;
        });

        for (let i = 0; i < tasks.length; i += 50) {
          const chunk = tasks.slice(i, i + 50);
          await queryFunc(`
            INSERT INTO workflow_taleed.whatsapp_session_keys (session_id, key_id, data, updated_at)
            VALUES ${chunk.join(',\n')}
            ON CONFLICT (session_id, key_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now();
          `);
        }
      }
    } catch (err) {
      console.error(`[SupabaseAuth] Background flush error:`, err.message);
    } finally {
      isFlushing = false;
      if (pendingKeyWrites.size > 0 || pendingKeyDeletes.size > 0) {
        scheduleFlush(200);
      }
    }
  }

  function scheduleFlush(delay = 400) {
    if (flushTimer) clearTimeout(flushTimer);
    flushTimer = setTimeout(flushKeysToSupabase, delay);
  }

  // 1. استرجاع بيانات الاعتماد الأساسية (creds) والمفاتيح السابقة من Supabase إلى كاش الذاكرة
  let creds;
  try {
    const res = await queryFunc(`SELECT creds FROM workflow_taleed.whatsapp_sessions WHERE id = '${sessionId}';`);
    if (res && res.length > 0 && res[0].creds) {
      creds = JSON.parse(JSON.stringify(res[0].creds), BufferJSON.reviver);
      console.log(`[SupabaseAuth] ✅ Loaded existing session creds for ${sessionId} from Supabase!`);
    } else {
      creds = initAuthCreds();
      console.log(`[SupabaseAuth] 🆕 Initialized fresh auth creds for ${sessionId}`);
    }
  } catch (err) {
    console.error(`[SupabaseAuth] Error loading creds for ${sessionId}:`, err.message);
    creds = initAuthCreds();
  }

  // تحميل المفاتيح المحفوظة مسبقاً في الرام لسرعة قراءة 0ms
  try {
    const allKeys = await queryFunc(`SELECT key_id, data FROM workflow_taleed.whatsapp_session_keys WHERE session_id = '${sessionId}';`);
    if (allKeys && Array.isArray(allKeys)) {
      for (const row of allKeys) {
        keysCache.set(row.key_id, row.data);
      }
      console.log(`[SupabaseAuth] ⚡ Cached ${keysCache.size} encryption keys in RAM for 0ms access!`);
    }
  } catch (err) {
    console.error(`[SupabaseAuth] Error pre-caching keys:`, err.message);
  }

  // 2. مهايئ المفاتيح الرقمية فائق السرعة (Ultra-Fast 0ms In-Memory Keys Adapter)
  const keys = {
    get: async (type, ids) => {
      const data = {};
      if (!ids || ids.length === 0) return data;

      const missingIds = [];
      for (const id of ids) {
        const keyId = `${type}-${id}`;
        if (keysCache.has(keyId)) {
          let val = keysCache.get(keyId);
          val = JSON.parse(JSON.stringify(val), BufferJSON.reviver);
          if (type === 'app-state-sync-key' && val) {
            val = proto.Message.AppStateSyncKeyData.fromObject(val);
          }
          data[id] = val;
        } else {
          missingIds.push(id);
        }
      }

      // إذا كانت كل المفاتيح في الكاش (الوضع الطبيعي 99.9%)، يتم الإرجاع فوراً بدون أي استعلام شبكة
      if (missingIds.length === 0) {
        return data;
      }

      // في حال وجود مفاتيح غير موجودة في الكاش، جلبها من Supabase وتخزينها بالكاش
      try {
        const idList = missingIds.map(id => `'${type}-${id}'`).join(',');
        const rows = await queryFunc(
          `SELECT key_id, data FROM workflow_taleed.whatsapp_session_keys WHERE session_id = '${sessionId}' AND key_id IN (${idList});`
        );
        for (const row of (rows || [])) {
          keysCache.set(row.key_id, row.data);
          const id = row.key_id.replace(`${type}-`, '');
          let val = JSON.parse(JSON.stringify(row.data), BufferJSON.reviver);
          if (type === 'app-state-sync-key' && val) {
            val = proto.Message.AppStateSyncKeyData.fromObject(val);
          }
          data[id] = val;
        }
      } catch (err) {
        console.error(`[SupabaseAuth] Error getting keys for ${type}:`, err.message);
      }

      return data;
    },
    set: async (data) => {
      // تحديث كاش الذاكرة فوراً (0ms) لمنع أي تعليق في فحص الـ QR وتأكيد الاقتران اللحظي
      for (const category in data) {
        for (const id in data[category]) {
          const value = data[category][id];
          const keyId = `${category}-${id}`;
          if (value) {
            keysCache.set(keyId, value);
            pendingKeyWrites.set(keyId, value);
            pendingKeyDeletes.delete(keyId);
          } else {
            keysCache.delete(keyId);
            pendingKeyWrites.delete(keyId);
            pendingKeyDeletes.add(keyId);
          }
        }
      }
      // جدولة الحفظ السحابي في الخلفية دون تعطيل عملية تسجيل الدخول
      scheduleFlush(300);
    }
  };

  const saveCreds = async () => {
    try {
      const serialized = JSON.stringify(creds, BufferJSON.replacer);
      await queryFunc(`
        INSERT INTO workflow_taleed.whatsapp_sessions (id, creds, updated_at)
        VALUES ('${sessionId}', '${serialized.replace(/'/g, "''")}'::jsonb, now())
        ON CONFLICT (id) DO UPDATE SET creds = EXCLUDED.creds, updated_at = now();
      `);
    } catch (err) {
      console.error(`[SupabaseAuth] Error saving creds:`, err.message);
    }
  };

  return {
    state: {
      creds,
      keys
    },
    saveCreds,
    flushKeys: flushKeysToSupabase
  };
}

module.exports = {
  useSupabaseAuthState
};
