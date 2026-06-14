const express = require('express');
const cors = require('cors');
const multer = require('multer');
const WebSocket = require('ws');
const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

// Configuration
const configPath = path.join(__dirname, 'config', 'server.json');
let config = {
    serverName: "MSC Multiplayer Server",
    port: 25565,
    maxPlayers: 10,
    wsPort: 25566
};

try {
    config = JSON.parse(fsSync.readFileSync(configPath, 'utf8'));
} catch (e) {
    console.log('Using default config');
}

// Initialize Express
const app = express();
const PORT = process.env.SERVER_PORT || config.port || 25565;
const WS_PORT = process.env.WS_PORT || config.wsPort || 25566;

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static('public'));

// Logging
function log(message) {
    const line = `[${new Date().toISOString()}] ${message}`;
    console.log(line);
    fs.appendFile('logs/server.log', line + '\n').catch(() => {});
}

// JSON Database
const DB_PATH = path.join(__dirname, 'data', 'server.json');
let database = { mods: [], players: [], whitelist: [] };

async function loadDatabase() {
    try {
        await fs.mkdir(path.join(__dirname, 'data'), { recursive: true });
        const content = await fs.readFile(DB_PATH, 'utf8').catch(() => null);
        if (content) {
            database = JSON.parse(content);
            if (!database.mods) database.mods = [];
            if (!database.players) database.players = [];
            if (!database.whitelist) database.whitelist = [];
        }
        log('Database loaded');
    } catch (e) {
        log('DB load error: ' + e.message);
    }
}

function saveDatabase() {
    fs.writeFile(DB_PATH, JSON.stringify(database, null, 2)).catch(() => {});
}

// Multer for file uploads
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const modsDir = path.join(__dirname, 'mods');
        fsSync.mkdirSync(modsDir, { recursive: true });
        cb(null, modsDir);
    },
    filename: (req, file, cb) => {
        cb(null, file.originalname);
    }
});
const upload = multer({ storage });

// WebSocket
let wss;
try {
    wss = new WebSocket.Server({ port: WS_PORT });
    wss.on('connection', (ws) => {
        log('WebSocket client connected');
        ws.on('close', () => log('WebSocket client disconnected'));
    });
    log(`WebSocket on port ${WS_PORT}`);
} catch (e) {
    log('WebSocket error: ' + e.message);
}

function broadcast(data) {
    if (wss) {
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify(data));
            }
        });
    }
}

// API Routes
app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/api/server-info', (req, res) => {
    res.json({
        serverName: process.env.SERVER_NAME || config.serverName,
        maxPlayers: parseInt(process.env.MAX_PLAYERS) || config.maxPlayers,
        currentPlayers: 0,
        mods: database.mods.map(m => ({
            name: m.name,
            version: m.version || '1.0',
            size: Math.round((m.size || 0) / 1024)
        }))
    });
});

app.get('/api/mods', (req, res) => {
    res.json(database.mods);
});

app.post('/api/mods', upload.array('mods', 10), async (req, res) => {
    try {
        if (!req.files || req.files.length === 0) {
            return res.status(400).json({ error: 'No files' });
        }

        const newMods = req.files.map(file => ({
            id: uuidv4(),
            name: file.originalname,
            filename: file.filename,
            size: file.size,
            version: '1.0',
            uploadedAt: new Date().toISOString(),
            downloads: 0
        }));

        database.mods = [...database.mods, ...newMods];
        saveDatabase();
        
        log(`Uploaded ${newMods.length} mods`);
        broadcast({ type: 'mods_updated', mods: newMods });
        
        res.json({ message: 'OK', mods: newMods });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/mods/:id', async (req, res) => {
    try {
        const modId = req.params.id;
        const mod = database.mods.find(m => m.id === modId || m.name === modId);
        
        if (!mod) return res.status(404).json({ error: 'Not found' });
        
        const modPath = path.join(__dirname, 'mods', mod.filename || mod.name);
        await fs.unlink(modPath).catch(() => {});
        
        database.mods = database.mods.filter(m => m.id !== modId && m.name !== modId);
        saveDatabase();
        
        log(`Deleted: ${mod.name}`);
        res.json({ message: 'Deleted' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/mods/:filename', async (req, res) => {
    try {
        const modPath = path.join(__dirname, 'mods', req.params.filename);
        if (!fsSync.existsSync(modPath)) {
            return res.status(404).json({ error: 'Not found' });
        }
        
        const mod = database.mods.find(m => m.filename === req.params.filename || m.name === req.params.filename);
        if (mod) {
            mod.downloads = (mod.downloads || 0) + 1;
            saveDatabase();
        }
        
        res.download(modPath);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// Start server
app.listen(PORT, '0.0.0.0', async () => {
    log('╔══════════════════════════════════════════════════════╗');
    log('║     My Summer Car Multiplayer Server v2.0            ║');
    log('╚══════════════════════════════════════════════════════╝');
    log(`Server running on http://0.0.0.0:${PORT}`);
    log(`Web Panel: http://localhost:${PORT}`);
    log(`Max Players: ${process.env.MAX_PLAYERS || config.maxPlayers}`);
    log('══════════════════════════════════════════════════════');
    
    await loadDatabase();
});
