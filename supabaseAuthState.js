const { proto, initAuthCreds, BufferJSON } = require('@whiskeysockets/baileys');

/**
 * Baileys Auth State Adapter using Supabase PostgreSQL
 * Ensures zero-data-loss and persistent WhatsApp sessions across cloud container restarts
 */
async function useSupabaseAuthState(queryFunc, sessionId = 'admin_instance_1') {
  // 1. استرجاع بيانات الاعتماد الأساسية (creds) من Supabase
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

  // 2. مهايئ المفاتيح الرقمية (Keys Adapter)
  const keys = {
    get: async (type, ids) => {
      const data = {};
      if (!ids || ids.length === 0) return data;

      try {
        const idList = ids.map(id => `'${type}-${id}'`).join(',');
        const rows = await queryFunc(
          `SELECT key_id, data FROM workflow_taleed.whatsapp_session_keys WHERE session_id = '${sessionId}' AND key_id IN (${idList});`
        );
        for (const row of (rows || [])) {
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
      const tasks = [];
      try {
        for (const category in data) {
          for (const id in data[category]) {
            const value = data[category][id];
            const keyId = `${category}-${id}`;
            if (value) {
              const serialized = JSON.stringify(value, BufferJSON.replacer);
              tasks.push(`('${sessionId}', '${keyId}', '${serialized.replace(/'/g, "''")}'::jsonb, now())`);
            } else {
              await queryFunc(`DELETE FROM workflow_taleed.whatsapp_session_keys WHERE session_id = '${sessionId}' AND key_id = '${keyId}';`);
            }
          }
        }
        if (tasks.length > 0) {
          for (let i = 0; i < tasks.length; i += 40) {
            const chunk = tasks.slice(i, i + 40);
            await queryFunc(`
              INSERT INTO workflow_taleed.whatsapp_session_keys (session_id, key_id, data, updated_at)
              VALUES ${chunk.join(',\n')}
              ON CONFLICT (session_id, key_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now();
            `);
          }
        }
      } catch (err) {
        console.error(`[SupabaseAuth] Error saving keys:`, err.message);
      }
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
    saveCreds
  };
}

module.exports = {
  useSupabaseAuthState
};
