const axios = require('axios');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Função robusta para converter datas da planilha (Coluna B - Created At)
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

// Formata data para o padrão brasileiro compensando o fuso
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
        // --- ETAPA 1: METADADOS E COLABORADORES ---
        console.log(">>> [ETAPA 1] Obtendo metadados...");
        const meta = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}`, { headers: gHeaders });
        const sheetHablla = meta.data.sheets.find(s => s.properties.title === "Base Hablla Card");
        if (!sheetHablla) throw new Error("Aba 'Base Hablla Card' não encontrada!");
        const idBaseHablla = sheetHablla.properties.sheetId;

        const resColab = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${DB_COLABORADOR_ID}/values/Base_de_Colaboradores!A:B`, { headers: gHeaders });
        const mapaNomes = {};
        if (resColab.data?.values) {
            resColab.data.values.forEach(r => { if (r[1]) mapaNomes[r[1]] = r[0]; });
        }

        // --- ETAPA 2: LOGIN HABLLA ---
        const login = await axios.post('https://api.hablla.com/v1/authentication/login', { email: HABLLA_EMAIL, password: HABLLA_PASSWORD });
        const hHeaders = { 'Authorization': `Bearer ${login.data.accessToken}` };

        const hoje = new Date();
        const seteDiasAtras = new Date();
        seteDiasAtras.setDate(hoje.getDate() - 7);
        seteDiasAtras.setHours(0, 0, 0, 0);

        // --- ETAPA 3: LIMPEZA SELETIVA (VARREDURA REVERSA COM TRAVA) ---
        console.log(">>> [ETAPA 3] Analisando registros recentes para limpeza...");
        const resSheet = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:B`, { headers: gHeaders });
        
        if (resSheet.data?.values) {
            const rows = resSheet.data.values;
            let blocosParaDeletar = [];
            let startIdx = -1;
            let contadorConsecutivasFora = 0;

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
                    if (contadorConsecutivasFora >= 20) break; // Trava de 20 linhas
                }
            }
            if (startIdx !== -1) blocosParaDeletar.push({ start: 1, end: startIdx + 1 });

            if (blocosParaDeletar.length > 0) {
                const requests = blocosParaDeletar.map(b => ({
                    deleteDimension: { range: { sheetId: idBaseHablla, dimension: "ROWS", startIndex: b.start, endIndex: b.end } }
                }));
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, { requests }, { headers: gHeaders });
                console.log(">>> Limpeza de segurança concluída.");
            }
        }

        // --- ETAPA 4: SINCRONIZAÇÃO COM FILTRO DE CRIAÇÃO ---
        console.log(">>> [ETAPA 4] Buscando novos cards na API...");
        let page = 1;
        while (true) {
            const resApi = await axios.get(`https://api.hablla.com/v3/workspaces/${HABLLA_WORKSPACE_ID}/cards`, {
                params: { board: HABLLA_BOARD_ID, limit: 50, page: page, updated_after: seteDiasAtras.toISOString() },
                headers: hHeaders
            });

            const cards = resApi.data.results || [];
            if (cards.length === 0) break;

            const rowsToInsert = cards
                .filter(c => new Date(c.created_at) >= seteDiasAtras) // Só entra se foi criado nos últimos 7 dias
                .map(card => {
                    let cf = ["", "", "", ""];
                    const ids = ["67b39131ee792966f3fba492", "67b608470787782ce7acafba", "67dc6a0a17925c23d8365708", "679120ec177ff6d2c7597156"];
                    (card.custom_fields || []).forEach(f => {
                        const idx = ids.indexOf(f.custom_field);
                        if (idx !== -1) cf[idx] = f.value;
                    });
                    const atendenteID = (card.user && typeof card.user === 'object') ? card.user.id : (card.user || "");

                    return [
                        formatarDataBR(card.updated_at), formatarDataBR(card.created_at), card.workspace, card.board, card.list,
                        cf[0], cf[1], cf[2], card.name, card.description, card.source, card.status,
                        atendenteID, formatarDataBR(card.finished_at), card.id, mapaNomes[atendenteID] || "", cf[3], (card.tags || []).map(t => t.name).join(", ")
                    ];
                });

            if (rowsToInsert.length > 0) {
                await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:A:append?valueInputOption=USER_ENTERED`, 
                    { values: rowsToInsert }, { headers: gHeaders });
                await sleep(1200);
            }

            const temCriacaoNova = cards.some(c => new Date(c.created_at) >= seteDiasAtras);
            if (!temCriacaoNova && page > 2) break; 
            page++;
            if (page > 500) break;
        }

        // --- ETAPA 5: FAXINA FINAL (REMOVER DUPLICADOS POR ID) ---
        console.log(">>> [ETAPA 5] Executando faxina de duplicados (mantendo a linha mais baixa)...");
        const resFaxina = await axios.get(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:R`, { headers: gHeaders });
        if (resFaxina.data?.values) {
            const rows = resFaxina.data.values;
            const cabecalho = rows[0];
            const dados = rows.slice(1);
            
            const mapaUnico = new Map();
            // De cima para baixo: IDs repetidos serão sobrescritos, mantendo o último (mais baixo)
            dados.forEach(linha => {
                const idCard = linha[14]; // Coluna O
                if (idCard) mapaUnico.set(idCard, linha);
            });

            const dadosLimpos = [cabecalho, ...mapaUnico.values()];

            await axios.post(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A:R:clear`, {}, { headers: gHeaders });
            await axios.put(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Base%20Hablla%20Card!A1`, 
                { values: dadosLimpos }, { params: { valueInputOption: 'USER_ENTERED' }, headers: gHeaders });
            console.log(`>>> Faxina concluída. Linhas únicas: ${dadosLimpos.length}`);
        }

        console.log(">>> [SUCESSO] Sincronização e faxina finalizadas.");

    } catch (e) {
        console.error("!!! ERRO NO PROCESSO !!!", e.message);
        process.exit(1);
    }
}

run();
