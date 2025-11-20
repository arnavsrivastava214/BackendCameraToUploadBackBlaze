require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const fsSync = require('fs');
const mysql = require('mysql2/promise');
const B2 = require('backblaze-b2');
const axios = require('axios');
const progress = require('progress-stream');
const { v4: uuidv4 } = require('uuid');
const http = require('http');
const https = require('https');

const app = express();
app.use(cors());
app.use(express.json());

const uploadDir = path.join(__dirname, 'uploads');

const storage = multer.diskStorage({
  destination: async (req, file, cb) => {
    await fs.mkdir(uploadDir, { recursive: true });
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    const safe = file.originalname.replace(/\s+/g, '_');
    const uniq = `${Date.now()}_${Math.round(Math.random()*1e6)}_${safe}`;
    cb(null, uniq);
  }
});
const upload = multer({ storage });

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10
});

const b2 = new B2({
  accountId: process.env.B2_ACCOUNT_ID,
  applicationKey: process.env.B2_APPLICATION_KEY
});

let cachedDownloadUrl = null;
let lastAuthTime = 0;
const AUTH_TTL_MS = 5 * 60 * 1000;

const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 20 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 20 });
const axiosInst = axios.create({ httpAgent, httpsAgent, timeout: 0, maxContentLength: Infinity, maxBodyLength: Infinity });

async function ensureAuthorized() {
  const now = Date.now();
  if (cachedDownloadUrl && (now - lastAuthTime) < AUTH_TTL_MS) return cachedDownloadUrl;
  const authRes = await b2.authorize();
  cachedDownloadUrl = authRes.data?.downloadUrl || null;
  lastAuthTime = Date.now();
  if (!cachedDownloadUrl) throw new Error('B2 authorize returned no downloadUrl');
  return cachedDownloadUrl;
}

async function getFreshUploadUrl() {
  const res = await b2.getUploadUrl({ bucketId: process.env.B2_BUCKET_ID });
  return { uploadUrl: res.data.uploadUrl, uploadAuthToken: res.data.authorizationToken };
}

const sseClients = new Map();
app.get('/events/:uploadId', (req, res) => {
  const uploadId = req.params.uploadId;
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  if (typeof res.flushHeaders === 'function') res.flushHeaders();
  res.write(`:ok\n\n`);
  sseClients.set(uploadId, res);
  req.on('close', () => sseClients.delete(uploadId));
});
function sendSse(uploadId, event, payload) {
  const res = sseClients.get(uploadId);
  if (!res) return;
  const data = JSON.stringify(payload);
  res.write(`event: ${event}\n`);
  data.split('\n').forEach(line => res.write(`data: ${line}\n`));
  res.write('\n');
}

async function uploadStreamToBackblaze(uploadUrl, uploadAuthToken, filePath, destFileName, uploadId) {
  const stat = await fs.stat(filePath);
  const total = stat.size;

  const readStream = fsSync.createReadStream(filePath, { highWaterMark: 64 * 1024 });

  const prog = progress({ length: total, time: 200 });
  readStream.pipe(prog);

  const headers = {
    Authorization: uploadAuthToken,
    'X-Bz-File-Name': encodeURIComponent(destFileName),
    'Content-Length': total,
    'X-Bz-Content-Sha1': 'do_not_verify',
    'Content-Type': 'b2/x-auto'
  };

  prog.on('progress', (p) => {
    sendSse(uploadId, 'progress', { fileName: destFileName, percent: Math.round(p.percentage) });
  });

  const axiosRes = await axiosInst.post(uploadUrl, prog, {
    headers,
    validateStatus: status => status >= 200 && status < 300
  });

  return axiosRes.data;
}

async function uploadFileToB2WithProgress(localPath, destFileName, mimeType, uploadId) {
  await ensureAuthorized();

  const attempt = async () => {
    const { uploadUrl, uploadAuthToken } = await getFreshUploadUrl();
    try {
      const b2Res = await uploadStreamToBackblaze(uploadUrl, uploadAuthToken, localPath, destFileName, uploadId);
      const fileUrl = `${cachedDownloadUrl}/file/${process.env.B2_BUCKET_NAME}/${encodeURIComponent(destFileName)}`;
      return { fileId: b2Res.fileId, fileName: destFileName, fileUrl, size: (await fs.stat(localPath)).size };
    } catch (err) {
      const status = err?.response?.status;
      if (status === 401) throw { retryable401: true, inner: err };
      throw err;
    }
  };

  try {
    return await attempt();
  } catch (err) {
    if (err?.retryable401) {
      cachedDownloadUrl = null;
      await ensureAuthorized();
      return await attempt();
    }
    throw err;
  }
}

function createWorkerPool(tasks, concurrency) {
  let i = 0;
  const results = [];
  const run = async () => {
    while (true) {
      const idx = i++;
      if (idx >= tasks.length) break;
      try {
        results[idx] = await tasks[idx]();
      } catch (err) {
        results[idx] = { error: err };
      }
    }
  };
  const workers = new Array(Math.min(concurrency, tasks.length)).fill(0).map(() => run());
  return Promise.all(workers).then(() => results);
}

app.post('/api/upload', upload.array('files', 1000), async (req, res) => {
  const uploadId = req.query.uploadId || uuidv4();
  if (!req.files || !req.files.length) return res.status(400).json({ message: 'No files uploaded', uploadId });

  try { await ensureAuthorized(); } catch (e) {
    console.error('authorize before workers failed', e);
    return res.status(500).json({ message: 'B2 authorize failed', error: String(e) });
  }

  const tasks = req.files.map(file => async () => {
    try {
      sendSse(uploadId, 'started', { fileName: file.filename, originalName: file.originalname });
      const b2res = await uploadFileToB2WithProgress(file.path, file.filename, file.mimetype, uploadId);
      const [insertRes] = await pool.execute(
        `INSERT INTO uploads (original_name, b2_file_name, b2_file_url, size, mime_type) VALUES (?, ?, ?, ?, ?)`,
        [file.originalname, b2res.fileName, b2res.fileUrl, b2res.size, file.mimetype]
      );
      await fs.unlink(file.path).catch(()=>{});
      sendSse(uploadId, 'done', { fileName: b2res.fileName, url: b2res.fileUrl });
      return { originalName: file.originalname, b2FileName: b2res.fileName, url: b2res.fileUrl, size: b2res.size, dbId: insertRes.insertId };
    } catch (err) {
      console.error('Upload error for', file.originalname, err?.response?.data || err.message || err);
      sendSse(uploadId, 'error', { fileName: file.filename, error: (err?.response?.data || err.message || String(err)) });
      await fs.unlink(file.path).catch(()=>{});
      return { originalName: file.originalname, error: err?.response?.data || err.message || String(err) };
    }
  });

  const CONCURRENCY = Number(process.env.UPLOAD_CONCURRENCY || 4);
  const results = await createWorkerPool(tasks, CONCURRENCY);

  sendSse(uploadId, 'finished', { results });
  return res.json({ success: true, uploadId, results });
});

const port = process.env.PORT || 4000;
app.listen(port, () => console.log(`Server listening on ${port}`));
