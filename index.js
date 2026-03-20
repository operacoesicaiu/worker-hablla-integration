const axios = require('axios');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Função robusta para tratar datas da planilha (ex: 19/3/2026 ou 19/03/2026)
function parseDataBR(texto) {
    if (!texto) return null;
    try {
        const limpo = texto.replace(',', '').trim().split(' ')[0];
        const [d, m, y] = limpo.split('/');
        if (!d || !m || !y) return null;
        const dataISO = `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}T00:00:00Z`;
        const dObj = new Date(dataISO);
        return isNaN(dObj.getTime()) ? null : dObj;
    } catch (e) {
        return null;
    }
}

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

    try {
        // --- 1. METADADOS E COLABORADORES ---
        console.log(`[${new Date().toISOString()}] Obtendo metadados e colaboradores...`);
        const meta = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}`, { headers: gHeaders });
        const sheetHablla = meta.data.sheets.find(s => s.properties.title === "Base Hablla Card");
        if (!sheetHablla) throw new Error("Aba 'Base Hablla Card' não encontrada!");
        const idBaseHablla = sheetHablla.properties.sheetId;

        const resDB = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:B`, { headers: gHeaders });
        const mapaNomes = {};
        if (resDB.data?.values) {
            resDB.data.values.forEach(row => { if (row[1]) mapaNomes[row[1]] = row[0]; });
        }

        // --- 2. LOGIN HABLLA ---
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- 3. LÓGICA DE DATAS ---
        const hoje = new Date();
        const agoraBR = new Date(hoje.getTime() - (3 * 3600000));
        const dataHojeBR = agoraBR.toLocaleDateString('pt-BR');
        const ehCargaInicial = dataHojeBR === '19/03/2026';

        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        seteDiasAtras.setHours(0, 0, 0, 0);

        // --- 4. LIMPEZA SELETIVA (LÓGICA DE 20 LINHAS) ---
        if (!ehCargaInicial) {
            console.log(`[${new Date().toISOString()}] Iniciando limpeza reversa (critério: 20 linhas)...`);
            const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, { headers: gHeaders });
            
            if (resSheet.data?.values) {
                const rows = resSheet.data.values;
                let blocosParaDeletar = [];
                let startIdx = -1;
                let contadorConsecutivasFora = 0;

                // Varredura de baixo para cima
                for (let i = rows.length - 1; i >= 1; i--) {
                    const dataRow = parseDataBR(rows[i][1]); // Coluna B

                    if (dataRow && dataRow >= seteDiasAtras) {
                        if (startIdx === -1) startIdx = i;
                        contadorConsecutivasFora = 0;
                    } else {
                        contadorConsecutivasFora++;
                        if (startIdx !== -1) {
                            blocosParaDeletar.push({ start: i + 1, end: startIdx + 1 });
                            startIdx = -1;
                        }
                        if (contadorConsecutivasFora >= 20) {
                            console.log(`[INFO] Parada atingida na linha ${i + 1}.`);
                            break;
                        }
                    }
                }
                if (startIdx !== -1) blocosParaDeletar.push({ start: 1, end: startIdx + 1 });

                if (blocosParaDeletar.length > 0) {
                    const requests = blocosParaDeletar.map(b => ({
                        deleteDimension: { range: { sheetId: idBaseHablla, dimension: "ROWS", startIndex: b.start, endIndex: b.end } }
                    }));
                    await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
                    console.log(`[OK] Limpeza concluída.`);
                }
            }
        }

        // --- 5. BUSCA E INSERÇÃO DE CARDS ---
        let page = 1;
        let paginasSemCriacaoNova = 0;
        const limiteCriacao = new Date();
        limiteCriacao.setDate(hoje.getDate() - 9);

        while (true) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { 
                    board: HABLLA_BOARD_ID, limit: 50, order: 'updated_at', page: page,
                    updated_after: !ehCargaInicial ? seteDiasAtras.toISOString() : undefined
                },
                headers: hHeaders
            });

            const cards = res.data.results || [];
            if (cards.length === 0) break;

            if (!ehCargaInicial) {
                const temCriacaoNova = cards.some(c => new Date(c.created_at) >= limiteCriacao);
                if (!temCriacaoNova) paginasSemCriacaoNova++;
                else paginasSemCriacaoNova = 0;
                if (paginasSemCriacaoNova >= 2) break;
            }

            console.log(`[${new Date().toISOString()}] Processando Cards página ${page}...`);

            const rowsCards = cards.map(card => {
                const fmt = (d) => d ? new Date(new Date(d).getTime() - (3 * 3600000)).toLocaleString('pt-BR', {timeZone: 'UTC'}).replace(',', '') : "";
                let cf = ["", "", "", ""];
                const ids = ["67b39131ee792966f3fba492", "67b608470787782ce7acafba", "67dc6a0a17925c23d8365708", "679120ec177ff6d2c7597156"];
                (card.custom_fields || []).forEach(f => {
                    const idx = ids.indexOf(f.custom_field);
                    if (idx !== -1) cf[idx] = f.value;
                });

                const atendenteID = (card.user && typeof card.user === 'object') ? card.user.id : (card.user || "");

                return [
                    fmt(card.updated_at), fmt(card.created_at), card.workspace, card.board, card.list,
                    cf[0], cf[1], cf[2], card.name, card.description, card.source, card.status,
                    atendenteID, fmt(card.finished_at), card.id, mapaNomes[atendenteID] || "", cf[3], (card.tags || []).map(t => t.name).join(", ")
                ];
            });

            if (rowsCards.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, 
                { values: rowsCards }, { headers: gHeaders });
                await sleep(1200);
            }
            if (page >= 500) break;
            page++;
        }

        // --- 6. ATENDENTES (Base Atendente) ---
        console.log(`[${new Date().toISOString()}] Processando Base Atendente...`);
        let dIni, dFim;
        if (ehCargaInicial) {
            dIni = "2026-01-01T00:00:00Z";
            dFim = agoraBR.toISOString();
        } else {
            const ontem = new Date(hoje);
            ontem.setDate(hoje.getDate() - 1);
            dIni = new Date(ontem.setHours(0,0,0,0)).toISOString();
            dFim = new Date(ontem.setHours(23,59,59,999)).toISOString();
        }

        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: dIni, end_date: dFim }, headers: hHeaders
        });

        const dataRef = new Date(dFim).toLocaleDateString('pt-BR');
        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [ 
                dataRef, HABLLA_WORKSPACE_ID, s.id || "", s.name || "", u.id || "", 
                mapaNomes[u.id] || "", u.email || "", item.total_services || 0, 
                item.tme || 0, item.tma || 0, c.id || "", c.name || "", c.type || "", 
                item.total_csat || 0, item.total_csat_greater_4 || 0, item.csat || 0, item.total_fcr || 0 
            ];
        });

        if (rowsAt.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, 
            { values: rowsAt }, { headers: gHeaders });
            console.log(`[OK] ${rowsAt.length} atendentes inseridos.`);
        }

        console.log(`[${new Date().toISOString()}] Tudo pronto!`);

    } catch (e) {
        console.error("--- ERRO NO PROCESSO ---");
        console.error(e.response ? JSON.stringify(e.response.data, null, 2) : e.message);
        process.exit(1);
    }
}
run();
