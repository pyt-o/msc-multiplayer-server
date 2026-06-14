const express = require('express');
const cors = require('cors');
const multer = require('multer');
const WebSocket = require('ws');
const fs = require('fs').promises;
const path = require('path');
const compression = require('compression');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { v4: uuidv4 } = require('uuid');
const cron = require('node-cron');
const sqlite3 = require('better-sqlite3');

// Configuration
const config = require('./config/server.json');

// Initialize Express
const app = express();
const PORT = process.env.SERVER_PORT || config.port || 25565;
const WS_PORT = process.env.WS_PORT || config.wsPort || 25566;

// Middleware
app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false
}));
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static('public'));

// Rate limiting
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 100
});
app.use('/api/', limiter);

// Logging
const logLevels = { debug: 0, info: 1, warn: 2, error: 3 };
const currentLogLevel = logLevels[process.env.LOG_LEVEL || config.logLevel] || 1;

function log(message, level = 'info') {
    if (logLevels[level] >= currentLogLevel) {
        const timestamp = new Date().toISOString();
        const logLine = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
        console.log(logLine);
        fs.appendFile('logs/server.log', logLine + '\n').catch(() => {});
    }
}

// Database initialization
let db;
if (process.env.ENABLE_DATABASE !== 'false' && config.enableDatabase) {
    try {
        db = sqlite3('data/server.db');
        db.pragma('journal_mode = WAL');
        
        db.exec(`
            CREATE TABLE IF NOT EXISTS mods (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                version TEXT,
                size INTEGER,
                uploadedAt TEXT,
                downloads INTEGER DEFAULT 0,
                checksum TEXT
            );
            
            CREATE TABLE IF NOT EXISTS players (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                firstSeen TEXT,
                lastSeen TEXT,
                playTime INTEGER DEFAULT 0
            );
            
            CREATE TABLE IF NOT EXISTS server_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                players_online INTEGER,
                memory_usage INTEGER,
                cpu_usage REAL,
                uptime INTEGER
            );
            
            CREATE TABLE IF NOT EXISTS whitelist (
                player_id TEXT PRIMARY KEY,
                added_by TEXT,
                added_at TEXT
            );
        `);
        
        log('Database initialized successfully', 'info');
    } catch (error) {
        log('Database initialization failed: ' + error.message, 'error');
    }
}

// Multer configuration
const storage = multer.diskStorage({
    destination: async (req, file, cb) => {
        const modsDir = path.join(__dirname, 'mods');
        await fs.mkdir(modsDir, { recursive: true });
        cb(null, modsDir);
    },
    filename: (req, file, cb) => {
        const uniqueName = `${uuidv4()}-${file.originalname}`;
        cb(null, uniqueName);
    }
});

const upload = multer({
    storage,
    limits: {
        fileSize: (process.env.MAX_UPLOAD_SIZE || config.maxUploadSize || 100) * 1024 * 1024
    },
    fileFilter: (req, file, cb) => {
        const allowedExts = (process.env.ALLOWED_MOD_EXTENSIONS || config.allowedModExtensions || '.dll,.unity3d,.assetbundle').split(',');
        const ext = path.extname(file.originalname).toLowerCase();
        if (allowedExts.includes(ext)) {
            cb(null, true);
        } else {
            cb(new Error('Invalid file extension'), false);
        }
    }
});

// WebSocket server
let wss;
if (process.env.ENABLE_WEBSOCKET !== 'false' && config.enableWebSocket) {
    wss = new WebSocket.Server({ port: WS_PORT });
    
    wss.on('connection', (ws) => {
        log('WebSocket client connected', 'debug');
        
        ws.on('message', (message) => {
            try {
                const data = JSON.parse(message);
                handleWebSocketMessage(ws, data);
            } catch (error) {
                log('WebSocket message error: ' + error.message, 'error');
            }
        });
        
        ws.on('close', () => {
            log('WebSocket client disconnected', 'debug');
        });
    });
    
    log(`WebSocket server running on port ${WS_PORT}`, 'info');
}

function handleWebSocketMessage(ws, data) {
    switch (data.type) {
        case 'subscribe':
            ws.subscriptions = data.channels || [];
            break;
        case 'ping':
            ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
            break;
    }
}

function broadcastToWebSocket(data) {
    if (wss) {
        wss.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify(data));
            }
        });
    }
}

// Server state
const serverState = {
    startTime: Date.now(),
    currentPlayers: 0,
    totalConnections: 0,
    mods: [],
    whitelist: []
};

// Load mods
async function loadMods() {
    try {
        if (db) {
            const rows = db.prepare('SELECT * FROM mods').all();
            serverState.mods = rows;
        } else {
            const modsDir = path.join(__dirname, 'mods');
            const files = await fs.readdir(modsDir).catch(() => []);
            serverState.mods = await Promise.all(files.map(async (file) => {
                const stats = await fs.stat(path.join(modsDir, file));
                return {
                    id: uuidv4(),
                    name: file,
                    size: stats.size,
                    uploadedAt: stats.birthtime.toISOString(),
                    version: '1.0',
                    downloads: 0
                };
            }));
        }
        log(`Loaded ${serverState.mods.length} mods`, 'info');
    } catch (error) {
        log('Error loading mods: ' + error.message, 'error');
    }
}

// API Routes

app.get('/api/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: Math.floor((Date.now() - serverState.startTime) / 1000)
    });
});

app.get('/api/server-info', (req, res) => {
    res.json({
        serverName: process.env.SERVER_NAME || config.serverName,
        description: process.env.SERVER_DESCRIPTION || config.description,
        maxPlayers: parseInt(process.env.MAX_PLAYERS) || config.maxPlayers,
        currentPlayers: serverState.currentPlayers,
        version: '2.0.0',
        features: {
            websocket: process.env.ENABLE_WEBSOCKET !== 'false',
            database: process.env.ENABLE_DATABASE !== 'false',
            backups: process.env.ENABLE_BACKUPS !== 'false'
        }
    });
});

app.get('/api/stats', (req, res) => {
    const uptime = Math.floor((Date.now() - serverState.startTime) / 1000);
    const memoryUsage = process.memoryUsage();
    
    res.json({
        uptime,
        players: {
            current: serverState.currentPlayers,
            total: serverState.totalConnections,
            max: parseInt(process.env.MAX_PLAYERS) || config.maxPlayers
        },
        memory: {
            used: Math.round(memoryUsage.heapUsed / 1024 / 1024),
            total: Math.round(memoryUsage.heapTotal / 1024 / 1024),
            rss: Math.round(memoryUsage.rss / 1024 / 1024)
        },
        mods: {
            count: serverState.mods.length,
            totalSize: serverState.mods.reduce((sum, mod) => sum + mod.size, 0)
        }
    });
});

app.get('/api/mods', async (req, res) => {
    try {
        await loadMods();
        res.json(serverState.mods);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/mods', upload.array('mods', 10), async (req, res) => {
    try {
        if (!req.files || req.files.length === 0) {
            return res.status(400).json({ error: 'No files uploaded' });
        }

        const uploadedMods = await Promise.all(req.files.map(async (file) => {
            const modData = {
                id: uuidv4(),
                name: file.originalname,
                filename: file.filename,
                size: file.size,
                version: '1.0',
                uploadedAt: new Date().toISOString(),
                downloads: 0,
                checksum: uuidv4()
            };
            
            if (db) {
                db.prepare(`
                    INSERT INTO mods (id, name, version, size, uploadedAt, downloads, checksum)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                `).run(modData.id, modData.name, modData.version, modData.size, 
                       modData.uploadedAt, modData.downloads, modData.checksum);
            }
            
            return modData;
        }));

        serverState.mods = [...serverState.mods, ...uploadedMods];
        
        log(`Uploaded ${uploadedMods.length} mods`, 'info');
        broadcastToWebSocket({ type: 'mods_updated', mods: uploadedMods });
        
        res.json({ 
            message: `Successfully uploaded ${uploadedMods.length} mods`,
            mods: uploadedMods
        });
    } catch (error) {
        log('Upload error: ' + error.message, 'error');
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/mods/:id', async (req, res) => {
    try {
        const modId = req.params.id;
        const mod = serverState.mods.find(m => m.id === modId || m.name === modId);
        
        if (!mod) {
            return res.status(404).json({ error: 'Mod not found' });
        }
        
        const modPath = path.join(__dirname, 'mods', mod.filename || mod.name);
        await fs.unlink(modPath).catch(() => {});
        
        if (db) {
            db.prepare('DELETE FROM mods WHERE id = ?').run(modId);
        }
        
        serverState.mods = serverState.mods.filter(m => m.id !== modId && m.name !== modId);
        
        log(`Deleted mod: ${mod.name}`, 'info');
        broadcastToWebSocket({ type: 'mod_deleted', modId });
        
        res.json({ message: 'Mod deleted successfully' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/mods/:filename', async (req, res) => {
    try {
        const modPath = path.join(__dirname, 'mods', req.params.filename);
        
        if (!await fs.access(modPath).then(() => true).catch(() => false)) {
            return res.status(404).json({ error: 'Mod not found' });
        }
        
        const mod = serverState.mods.find(m => m.filename === req.params.filename || m.name === req.params.filename);
        if (mod) {
            mod.downloads = (mod.downloads || 0) + 1;
            if (db) {
                db.prepare('UPDATE mods SET downloads = ? WHERE id = ?')
                  .run(mod.downloads, mod.id);
            }
        }
        
        res.download(modPath);
        log(`Mod downloaded: ${req.params.filename}`, 'debug');
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/backup', async (req, res) => {
    try {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupFile = `backups/backup_${timestamp}.tar.gz`;
        
        await fs.mkdir('backups', { recursive: true });
        
        const { exec } = require('child_process');
        await new Promise((resolve, reject) => {
            exec(`tar -czf ${backupFile} mods config data 2>/dev/null`, (error) => {
                if (error) reject(error);
                else resolve();
            });
        });
        
        const backups = await fs.readdir('backups');
        const sortedBackups = backups.sort().reverse();
        for (let i = 10; i < sortedBackups.length; i++) {
            await fs.unlink(path.join('backups', sortedBackups[i]));
        }
        
        log('Backup created: ' + backupFile, 'info');
        res.json({ message: 'Backup created', file: backupFile });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/logs', async (req, res) => {
    try {
        const logs = await fs.readFile('logs/server.log', 'utf8').catch(() => '');
        const lines = logs.split('\n').slice(-100);
        res.json({ logs: lines });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/whitelist', (req, res) => {
    if (db) {
        const entries = db.prepare('SELECT * FROM whitelist').all();
        res.json(entries);
    } else {
        res.json(serverState.whitelist);
    }
});

app.post('/api/whitelist', (req, res) => {
    const { playerId, addedBy } = req.body;
    if (!playerId) {
        return res.status(400).json({ error: 'playerId required' });
    }
    
    if (db) {
        db.prepare('INSERT OR REPLACE INTO whitelist (player_id, added_by, added_at) VALUES (?, ?, ?)')
          .run(playerId, addedBy || 'system', new Date().toISOString());
    }
    
    serverState.whitelist.push({ playerId, addedBy: addedBy || 'system', addedAt: new Date().toISOString() });
    res.json({ message: 'Player added to whitelist' });
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// Scheduled tasks
if (process.env.ENABLE_BACKUPS !== 'false' && config.enableBackups) {
    const interval = parseInt(process.env.BACKUP_INTERVAL) || config.backupInterval || 6;
    cron.schedule(`0 */${interval} * * *`, async () => {
        log('Running scheduled backup...', 'info');
        try {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const backupFile = `backups/backup_${timestamp}.tar.gz`;
            await fs.mkdir('backups', { recursive: true });
            
            const { exec } = require('child_process');
            await new Promise((resolve, reject) => {
                exec(`tar -czf ${backupFile} mods config data 2>/dev/null`, (error) => {
                    if (error) reject(error);
                    else resolve();
                });
            });
            
            log('Scheduled backup completed', 'info');
        } catch (error) {
            log('Scheduled backup failed: ' + error.message, 'error');
        }
    });
}

cron.schedule('*/5 * * * *', () => {
    if (db) {
        const memoryUsage = process.memoryUsage();
        db.prepare(`
            INSERT INTO server_stats (timestamp, players_online, memory_usage, cpu_usage, uptime)
            VALUES (?, ?, ?, ?, ?)
        `).run(
            new Date().toISOString(),
            serverState.currentPlayers,
            memoryUsage.heapUsed,
            0,
            Math.floor((Date.now() - serverState.startTime) / 1000)
        );
    }
});

if (process.env.AUTO_UPDATE_MODS !== 'false' && config.autoUpdateMods) {
    cron.schedule('0 0 * * *', async () => {
        log('Checking for mod updates...', 'info');
    });
}

process.on('SIGTERM', async () => {
    log('Shutting down gracefully...', 'info');
    if (db) db.close();
    if (wss) wss.close();
    process.exit(0);
});

// Start server
app.listen(PORT, process.env.SERVER_IP || '0.0.0.0', () => {
    log('╔══════════════════════════════════════════════════════╗', 'info');
    log('║     My Summer Car Multiplayer Server v2.0            ║', 'info');
    log('╚══════════════════════════════════════════════════════╝', 'info');
    log(`Server running on http://${process.env.SERVER_IP || '0.0.0.0'}:${PORT}`, 'info');
    log(`Web Panel: http://localhost:${PORT}`, 'info');
    if (config.enableWebSocket) {
        log(`WebSocket: ws://localhost:${WS_PORT}`, 'info');
    }
    log(`Max Players: ${process.env.MAX_PLAYERS || config.maxPlayers}`, 'info');
    log(`Server Name: ${process.env.SERVER_NAME || config.serverName}`, 'info');
    log('══════════════════════════════════════════════════════', 'info');
    
    loadMods();
    
    if (process.env.ENABLE_BACKUPS !== 'false' && config.enableBackups) {
        setTimeout(async () => {
            await fs.mkdir('backups', { recursive: true });
            log('Backup system enabled', 'info');
        }, 5000);
    }
});

module.exports = app;