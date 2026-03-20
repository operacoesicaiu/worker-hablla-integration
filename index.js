const axios = require('axios');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function parseDataBR(texto) {
    if (!texto) return null;
    try {
        const limpo = texto.replace(',', '').trim().split(' ')[0];
        const [d, m, y] = limpo.split('/');
        if (!d || !m || !y) return null;
        const dataISO = `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}T00:00:00Z`;
        const dObj = new Date(dataISO);
        return isNaN(dObj.getTime()) ? null : dObj;
    } catch (e) { return null; }
}

function formatarDataBR(dataISO) {
    if (!dataISO) return "";
    return new Date(new Date(dataISO).getTime() - (3 * 3600000))
        .toLocaleString('pt-BR', { timeZone: 'UTC' })
        .replace(',', '');
}

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

    try {
        // --- 1. PREPARAÇÃO ---
        console.log(">>> [ETAPA 1] Metadados e Colaboradores...");
        const meta = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}`, { headers: gHeaders });
        const sheetHablla = meta.data.sheets.find(s => s.properties.title === "Base Hablla Card");
        const idBaseHablla = sheetHablla.properties.sheetId;

        const resColab = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:B`, { headers: gHeaders });
        const mapaNomes = {};
        if (resColab.data?.values) {
            resColab.data.values.forEach(r => { if (r[1]) mapaNomes[r[1]] = r[0]; });
        }

        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        const hoje = new Date();
        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        seteDiasAtras.setHours(0, 0, 0, 0);

        // --- 2. LIMPEZA SEGURANÇA (7 DIAS) ---
        console.log(">>> [ETAPA 2] Limpeza de segurança (7 dias)...");
        const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, { headers: gHeaders });
        if (resSheet.data?.values) {
            const rows = resSheet.data.values;
            let blocos = [], startIdx = -1, cont = 0;
            for (let i = rows.length - 1; i >= 1; i--) {
                const dt = parseDataBR(rows[i][1]);
                if (dt && dt >= seteDiasAtras) { if (startIdx === -1) startIdx = i; cont = 0; }
                else { cont++; if (startIdx !== -1) { blocos.push({ start: i + 1, end: startIdx + 1 }); startIdx = -1; } if (cont >= 20) break; }
            }
            if (startIdx !== -1) blocos.push({ start: 1, end: startIdx + 1 });
            if (blocos.length > 0) {
                const requests = blocos.map(b => ({ deleteDimension: { range: { sheetId: idBaseHablla, dimension: "ROWS", startIndex: b.start, endIndex: b.end } } }));
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
            }
        }

        // --- 3. SINCRONIZAÇÃO CARDS ---
        console.log(">>> [ETAPA 3] Sincronizando Cards...");
        let page = 1;
        while (true) {
            const resApi = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, limit: 50, page: page, updated_after: seteDiasAtras.toISOString() },
                headers: hHeaders
            });
            const cards = resApi.data.results || [];
            if (cards.length === 0) break;

            const rowsToInsert = cards.filter(c => new Date(c.created_at) >= seteDiasAtras).map(card => {
                let cf = ["", "", "", ""];
                const ids = ["67b39131ee792966f3fba492", "67b608470787782ce7acafba", "67dc6a0a17925c23d8365708", "679120ec177ff6d2c7597156"];
                (card.custom_fields || []).forEach(f => { const idx = ids.indexOf(f.custom_field); if (idx !== -1) cf[idx] = f.value; });
                const uid = (card.user && typeof card.user === 'object') ? card.user.id : (card.user || "");
                return [
                    formatarDataBR(card.updated_at), formatarDataBR(card.created_at), card.workspace, card.board, card.list,
                    cf[0], cf[1], cf[2], card.name, card.description, card.source, card.status,
                    uid, formatarDataBR(card.finished_at), card.id, mapaNomes[uid] || "", cf[3], (card.tags || []).map(t => t.name).join(", ")
                ];
            });

            if (rowsToInsert.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, { values: rowsToInsert }, { headers: gHeaders });
                await sleep(1200);
            }
            if (!cards.some(c => new Date(c.created_at) >= seteDiasAtras) && page > 2) break;
            page++; if (page > 500) break;
        }

        // --- 4. FAXINA DUPLICADOS ---
        console.log(">>> [ETAPA 4] Faxina de duplicados...");
        const resF = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:R`, { headers: gHeaders });
        if (resF.data?.values) {
            const rows = resF.data.values, mapU = new Map();
            rows.slice(1).forEach(l => { if (l[14]) mapU.set(l[14], l); });
            const final = [rows[0], ...mapU.values()];
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:R:clear`, {}, { headers: gHeaders });
            await axios.put(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A1`, { values: final }, { params: { valueInputOption: 'USER_ENTERED' }, headers: gHeaders });
        }

        // --- 5. BASE ATENDENTE (RELATÓRIO DE ONTEM) ---
        console.log(">>> [ETAPA 5] Processando Base Atendente...");
        const ontem = new Date(); ontem.setDate(ontem.getDate() - 1);
        const dIni = new Date(ontem.setHours(0,0,0,0)).toISOString();
        const dFim = new Date(ontem.setHours(23,59,59,999)).toISOString();

        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: dIni, end_date: dFim },
            headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [ 
                new Date(dFim).toLocaleDateString('pt-BR'), HABLLA_WORKSPACE_ID, s.id || "", s.name || "", u.id || "", 
                mapaNomes[u.id] || "", u.email || "", item.total_services || 0, 
                item.tme || 0, item.tma || 0, c.id || "", c.name || "", c.type || "", 
                item.total_csat || 0, item.total_csat_greater_4 || 0, item.csat || 0, item.total_fcr || 0 
            ];
        });

        if (rowsAt.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, 
                { values: rowsAt }, { headers: gHeaders });
            console.log(`>>> [OK] ${rowsAt.length} linhas de atendentes inseridas.`);
        }

        console.log(">>> [SUCESSO] Processo Geral Concluído.");

    } catch (e) { 
        console.error("Erro na API: ", e.response.status)
    }
}
run();
