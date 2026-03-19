const axios = require('axios');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    try {
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

        // 1. SINCRONIZA COLABORADORES (Busca ID na Coluna M [12] e Nome na Coluna A [0])
        console.log(`[${new Date().toISOString()}] Lendo Base_de_Colaboradores...`);
        const resDB = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:B`, { headers: gHeaders });
        const mapaNomes = {};
        if (resDB.data?.values) {
            resDB.data.values.forEach(row => { 
                if (row[1]) mapaNomes[row[1]] = row[0]; 
            });
        }

        // 2. LOGIN HABLLA
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- LÓGICA DE DATAS ---
        const hoje = new Date();
        const agoraBR = new Date(hoje.getTime() - (3 * 3600000));
        const dataHojeBR = agoraBR.toLocaleDateString('pt-BR');
        const ehCargaInicial = dataHojeBR === '19/03/2026';

        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        seteDiasAtras.setHours(0, 0, 0, 0);

        const limiteCriacao = new Date();
        limiteCriacao.setDate(hoje.getDate() - 9); 

        // 3. LIMPEZA SELETIVA (Apenas se não for carga inicial)
        if (!ehCargaInicial) {
            console.log(`[${new Date().toISOString()}] Limpando registros recentes na Base Hablla Card...`);
            const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, { headers: gHeaders });
            if (resSheet.data?.values) {
                const indicesParaDeletar = resSheet.data.values
                    .map((row, index) => {
                        const dataCriacaoStr = row[1]; // Coluna B
                        if (!dataCriacaoStr || index === 0) return -1;
                        const [d, m, y] = dataCriacaoStr.split(' ')[0].split('/');
                        const dataRow = new Date(`${y}-${m}-${d}T00:00:00Z`);
                        return dataRow >= seteDiasAtras ? index : -1;
                    })
                    .filter(i => i !== -1);

                if (indicesParaDeletar.length > 0) {
                    const requests = indicesParaDeletar.reverse().map(i => ({
                        deleteDimension: { range: { sheetId: 0, dimension: "ROWS", startIndex: i, endIndex: i + 1 } }
                    }));
                    await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
                    console.log(`[OK] Limpeza de ${indicesParaDeletar.length} linhas concluída.`);
                }
            }
        }

        // 4. BUSCA CARDS (Lógica e Mapeamento idênticos ao Script 1)
        let page = 1;
        let continuarBuscando = true;
        let paginasSemCriacaoNova = 0;

        while (continuarBuscando) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { 
                    board: HABLLA_BOARD_ID, 
                    limit: 50, 
                    order: 'updated_at', 
                    page: page,
                    updated_after: !ehCargaInicial ? seteDiasAtras.toISOString() : undefined
                },
                headers: hHeaders
            });

            const cards = res.data.results || [];
            if (cards.length === 0) break;

            // Trava de segurança para rotina diária
            if (!ehCargaInicial) {
                const temCriacaoNova = cards.some(c => new Date(c.created_at) >= limiteCriacao);
                if (!temCriacaoNova) paginasSemCriacaoNova++;
                else paginasSemCriacaoNova = 0;

                if (paginasSemCriacaoNova >= 2) {
                    console.log(`[STOP] Fim da janela de atualizações.`);
                    break;
                }
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

                // ID do Atendente (Garante extração correta se for objeto ou string)
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
            
            if (page >= 500) break; // Trava contra loops infinitos
            page++;
        }

        // 5. ATENDENTES (Base Atendente)
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
            params: { start_date: dIni, end_date: dFim },
            headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            const dataRef = new Date(dFim).toLocaleDateString('pt-BR');
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
            console.log(`[OK] ${rowsAt.length} linhas de atendentes inseridas.`);
        }

        console.log(`[${new Date().toISOString()}] Tudo pronto!`);

    } catch (e) {
        console.error("--- ERRO NO PROCESSO ---");
        if (e.response) console.error(JSON.stringify(e.response.data, null, 2));
        else console.error(e.message);
        process.exit(1);
    }
}
run();
