const axios = require('axios');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function run() {
    const { 
        GOOGLE_TOKEN, HABLLA_EMAIL, HABLLA_PASSWORD, 
        HABLLA_WORKSPACE_ID, HABLLA_BOARD_ID, SPREADSHEET_ID, DB_COLABORADOR_ID 
    } = process.env;

    try {
        const gHeaders = { 'Authorization': `Bearer ${GOOGLE_TOKEN}`, 'Content-Type': 'application/json' };

        // 1. Sincroniza Colaboradores (Usando a lógica da Coluna 12/M da planilha de colaboradores)
        console.log(`[${new Date().toISOString()}] Sincronizando base de colaboradores...`);
        const resDB = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:M`, { headers: gHeaders });
        const mapaNomes = {};
        if (resDB.data?.values) {
            resDB.data.values.forEach(row => { 
                if (row[12]) mapaNomes[row[12]] = row[0]; 
            });
        }

        // 2. Login Hablla
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        // --- LÓGICA DE DATA PARA GITHUB ACTIONS (UTC-3) ---
        const agoraBR = new Date(new Date().getTime() - (3 * 3600000));
        const dia = String(agoraBR.getUTCDate()).padStart(2, '0');
        const mes = String(agoraBR.getUTCMonth() + 1).padStart(2, '0');
        const ano = agoraBR.getUTCFullYear();
        const dataHojeBR = `${dia}/${mes}/${ano}`;

        const ehCargaInicial = dataHojeBR === '19/03/2026';
        console.log(`[INFO] Data BR: ${dataHojeBR}. Carga Inicial: ${ehCargaInicial}`);

        // Datas para controle de Cards
        const seteDiasAtras = new Date(agoraBR);
        seteDiasAtras.setDate(agoraBR.getDate() - 7);
        const limiteCriacao = new Date(agoraBR);
        limiteCriacao.setDate(agoraBR.getDate() - 9); 

        // 3. Busca de Cards
        let page = 1;
        let continuarBuscando = true;
        while (continuarBuscando) {
            const res = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, page, limit: 50, order: "updated_at" }, 
                headers: hHeaders
            });
            const cards = res.data.results || [];
            if (cards.length === 0) break;

            // Trava de segurança para não varrer o histórico inteiro em dias comuns
            if (!ehCargaInicial) {
                const temCriacaoNova = cards.some(c => new Date(c.created_at) >= limiteCriacao);
                if (!temCriacaoNova && page > 2) break; 
            }

            const rows = cards
                .filter(c => ehCargaInicial || new Date(c.updated_at) >= seteDiasAtras)
                .map(card => {
                    const dtUp = new Date(card.updated_at);
                    const atualizadoEm = new Date(dtUp.getTime() - (3 * 3600000)).toLocaleString('pt-BR').replace(',', '');
                    
                    // --- IGUALANDO À LÓGICA DO SEU SEGUNDO SCRIPT ---
                    // Se card.user for um objeto, pegamos o .id. Se for string, usamos a string.
                    const atendenteID = (card.user && typeof card.user === 'object') ? card.user.id : (card.user || "");
                    
                    return [
                        atualizadoEm, 
                        card.created_at, 
                        card.id, 
                        card.name, 
                        card.status, 
                        card.list,
                        atendenteID,               // Coluna G: Recebe o ID igual à Coluna M do outro script
                        mapaNomes[atendenteID] || "", // Coluna H: Busca o nome usando esse ID
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

        // 4. LÓGICA DE ATENDENTES (Base Atendente)
        console.log(`[${new Date().toISOString()}] Processando Base Atendente...`);
        
        let dataInicioRelatorio, dataFimRelatorio;

        if (ehCargaInicial) {
            dataInicioRelatorio = "2026-01-01T00:00:00Z";
            dataFimRelatorio = agoraBR.toISOString(); 
        } else {
            const ontem = new Date(agoraBR);
            ontem.setDate(agoraBR.getDate() - 1);
            dataInicioRelatorio = new Date(ontem.setHours(0,0,0,0)).toISOString();
            dataFimRelatorio = new Date(ontem.setHours(23,59,59,999)).toISOString();
        }

        const resAt = await axios.get(`https://api.hablla.com/v1/workspaces/${HABLLA_WORKSPACE_ID}/reports/services/summary`, {
            params: { start_date: dataInicioRelatorio, end_date: dataFimRelatorio },
            headers: hHeaders
        });

        const rowsAt = (resAt.data.results || []).map(item => {
            const u = item.user || {}, s = item.sector || {}, c = item.connection || {};
            const atendenteID_Relatorio = u.id || ""; // ID vindo do relatório summary
            
            return [ 
                new Date(dataFimRelatorio).toLocaleDateString('pt-BR'), 
                HABLLA_WORKSPACE_ID, 
                s.id || "", 
                s.name || "", 
                atendenteID_Relatorio, 
                mapaNomes[atendenteID_Relatorio] || "", // Busca nome pelo ID do relatório
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
            console.log(`[OK] Inseridas ${rowsAt.length} linhas de atendentes.`);
        }

    } catch (err) {
        console.error("ERRO:", err.response?.data || err.message);
        process.exit(1);
    }
}
run();
