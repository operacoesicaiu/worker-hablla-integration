const axios = require('axios');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    try {
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

        // 1. Sincroniza Colaboradores (Mapa de ID para Nome)
        console.log(`[${new Date().toISOString()}] Lendo base de colaboradores...`);
        const resDB = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/A:B`, { headers: gHeaders });
        const mapaNomes = {};
        if (resDB.data?.values) {
            resDB.data.values.forEach(row => {
                if (row[1] && row[0]) mapaNomes[row[1]] = row[0];
            });
        }

        // 2. Login Hablla
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- LÓGICA DE TRANSIÇÃO ---
        const hoje = new Date();
        const ehCargaInicial = hoje.toLocaleDateString('pt-BR') === '19/03/2026';
        
        // Datas para Cards
        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        const limiteCriacao = new Date();
        limiteCriacao.setDate(hoje.getDate() - 9); 

        // 3. Limpeza da Planilha (Aba Cards)
        if (!ehCargaInicial) {
            console.log(`[${new Date().toISOString()}] Rotina: Limpando Cards dos últimos 7 dias...`);
            const resCardsAtuais = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A`, { headers: gHeaders });
            if (resCardsAtuais.data?.values) {
                const indicesParaDeletar = resCardsAtuais.data.values
                    .map((row, index) => {
                        if (!row[0] || index === 0) return -1;
                        const partes = row[0].split(' ')[0].split('/'); 
                        const dataRow = new Date(`${partes[2]}-${partes[1]}-${partes[0]}T00:00:00Z`);
                        return dataRow >= seteDiasAtras ? index : -1;
                    })
                    .filter(i => i !== -1);

                if (indicesParaDeletar.length > 0) {
                    const requests = indicesParaDeletar.reverse().map(i => ({
                        deleteDimension: { range: { sheetId: 0, dimension: "ROWS", startIndex: i, endIndex: i + 1 } }
                    }));
                    await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
                }
            }
        }

        // 4. Busca de Cards
        let page = 1;
        let continuarBuscando = true;
        while (continuarBuscando) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, page, limit: 50, order: "updated_at" }, 
                headers: hHeaders
            });
            const cards = res.data.results || [];
            if (cards.length === 0) break;

            if (!ehCargaInicial) {
                const temCriacaoNova = cards.some(c => new Date(c.created_at) >= limiteCriacao);
                if (!temCriacaoNova && page > 2) break; 
            }

            const rows = cards
                .filter(c => ehCargaInicial || new Date(c.updated_at) >= seteDiasAtras)
                .map(card => {
                    const dtUp = new Date(card.updated_at);
                    const atualizadoEm = new Date(dtUp.getTime() - (3 * 3600000)).toLocaleString('pt-BR').replace(',', '');
                    
                    // --- CORREÇÃO: EXTRAÇÃO DO ID DO ATENDENTE ---
                    const atendenteID = (card.user && typeof card.user === 'object') ? card.user.id : (card.user || "");
                    
                    return [
                        atualizadoEm, 
                        card.created_at, 
                        card.id, 
                        card.name, 
                        card.status, 
                        card.list,
                        atendenteID,               // Coluna G: Preenche o ID (Texto)
                        mapaNomes[atendenteID] || "", // Coluna H: Busca o Nome no mapa
                        (card.tags || []).map(t => t.name).join(", "),
                        (card.custom_fields || []).filter(f => f.value).map(f => String(f.value)).join(" | ")
                    ];
                });

            if (rows.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, { values: rows }, { headers: gHeaders });
            }
            if (page >= 1000) break;
            page++;
            await sleep(500);
        }

        // 5. LÓGICA DE ATENDENTES
        console.log(`[${new Date().toISOString()}] Iniciando processamento de Atendentes...`);
        
        let dataInicioRelatorio, dataFimRelatorio;

        if (ehCargaInicial) {
            dataInicioRelatorio = "2026-01-01T00:00:00Z";
            const ontem = new Date(); ontem.setDate(hoje.getDate() - 1);
            dataFimRelatorio = new Date(ontem.setHours(23,59,59,999)).toISOString();
        } else {
            const ontem = new Date(); ontem.setDate(hoje.getDate() - 1);
            dataInicioRelatorio = new Date(ontem.setHours(0,0,0,0)).toISOString();
            dataFimRelatorio = new Date(ontem.setHours(23,59,59,999)).toISOString();
        }

        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: dataInicioRelatorio, end_date: dataFimRelatorio },
            headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            const dataRef = new Date(dataFimRelatorio).toLocaleDateString('pt-BR');
            
            return [ 
                dataRef, 
                HABLLA_WORKSPACE_ID, 
                s.id || "", 
                s.name || "", 
                u.id || "", 
                mapaNomes[u.id] || "", 
                u.email || "", 
                item.total_services || 0, 
                item.tme || 0, 
                item.tma || 0, 
                c.id || "", 
                c.name || "", 
                c.type || "", 
                item.total_csat || 0, 
                item.total_csat_greater_4 || 0, 
                item.csat || 0, 
                item.total_fcr || 0 
            ];
        });

        if (rowsAt.length > 0) {
            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Atendente!A:A:append?valueInputOption=USER_ENTERED`, 
            { values: rowsAt }, { headers: gHeaders });
            console.log(`[OK] ${rowsAt.length} linhas de atendentes inseridas.`);
        }

        console.log(`[${new Date().toISOString()}] Tudo pronto!`);

    } catch (err) {
        console.error("ERRO NO PROCESSO:", err.response?.data || err.message);
        process.exit(1);
    }
}

run();
