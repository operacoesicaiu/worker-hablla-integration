const axios = require('axios');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    try {
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

        // 1. SINCRONIZA COLABORADORES (Igual ao seu script que funciona)
        console.log(`[${new Date().toISOString()}] Lendo Base_de_Colaboradores...`);
        const resDB = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`, { headers: gHeaders });
        const mapaNomes = {};
        if (resDB.data?.values) {
            // Mapeia a Coluna M (índice 12) para o Nome na Coluna A (índice 0)
            resDB.data.values.forEach(row => { if (row[12]) mapaNomes[row[12]] = row[0]; });
        }

        // 2. LOGIN HABLLA
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- LÓGICA DE DATA (Ajustada para GitHub Actions UTC-3) ---
        const agoraBR = new Date(new Date().getTime() - (3 * 3600000));
        const dataHojeBR = agoraBR.toLocaleDateString('pt-BR');
        const ehCargaInicial = dataHojeBR === '19/03/2026';

        // 3. BUSCA CARDS (Migrado para a lógica do script que funciona)
        let page = 1;
        let continuarBuscando = true;
        
        while (continuarBuscando) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, page, limit: 50, order: "updated_at" }, 
                headers: hHeaders
            });
            const cards = res.data.results || [];
            if (cards.length === 0) break;

            const rowsCards = cards.map(card => {
                // Função de formatação de data igual ao seu script funcional
                const fmt = (d) => d ? new Date(new Date(d).getTime() - (3 * 3600000)).toLocaleString('pt-BR').replace(',', '') : "";
                
                // Lógica de Custom Fields (IDs específicos que você usa)
                let cf = ["", "", "", ""];
                const ids = ["67b39131ee792966f3fba492", "67b608470787782ce7acafba", "67dc6a0a17925c23d8365708", "679120ec177ff6d2c7597156"];
                (card.custom_fields || []).forEach(f => {
                    const idx = ids.indexOf(f.custom_field);
                    if (idx !== -1) cf[idx] = f.value;
                });

                // Extração do ID do Atendente (Garante que pegue o ID puro para a Coluna G e M)
                const atendenteID = (card.user && typeof card.user === 'object') ? card.user.id : (card.user || "");

                return [
                    fmt(card.updated_at),  // A
                    fmt(card.created_at),  // B
                    card.workspace,        // C
                    card.board,            // D
                    card.list,             // E
                    cf[0],                 // F
                    cf[1],                 // G
                    cf[2],                 // H
                    card.name,             // I
                    card.description,      // J
                    card.source,           // K
                    card.status,           // L
                    atendenteID,           // M (Antiga G) - Aqui entra o ID que você pediu
                    fmt(card.finished_at), // N
                    card.id,               // O
                    mapaNomes[atendenteID] || "", // P - Nome via Mapa
                    cf[3],                 // Q
                    (card.tags || []).map(t => t.name).join(", ") // R
                ];
            });

            if (rowsCards.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, 
                { values: rowsCards }, { headers: gHeaders });
            }
            
            // Trava para não buscar infinito se não for dia de carga
            if (!ehCargaInicial && page > 3) break; 
            page++;
            await sleep(1000);
        }

        // 4. ATENDENTES (Base Atendente - Agora pegando período total se for dia 19/03)
        console.log(`[${new Date().toISOString()}] Processando Base Atendente...`);
        let dIni, dFim;

        if (ehCargaInicial) {
            dIni = "2026-01-01T00:00:00Z";
            dFim = agoraBR.toISOString();
        } else {
            const ontem = new Date(agoraBR);
            ontem.setDate(agoraBR.getDate() - 1);
            dIni = new Date(ontem.setHours(0,0,0,0)).toISOString();
            dFim = new Date(ontem.setHours(23,59,59,999)).toISOString();
        }

        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: dIni, end_date: dFim },
            headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            return [ 
                new Date(dFim).toLocaleDateString('pt-BR'), 
                HABLLA_WORKSPACE_ID, s.id || "", s.name || "", u.id || "", 
                mapaNomes[u.id] || "", u.email || "", item.total_services || 0, 
                item.tme || 0, item.tma || 0, c.id || "", c.name || "", c.type || "", 
                item.total_csat || 0, item.total_csat_
