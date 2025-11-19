// server.js (SSE + server->B2 progress with axios + progress-stream)
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

const app = express();
app.use(cors());
app.use(express.json());

const uploadDir = path.join(__dirname, 'uploads');

// Multer - store temporarily on disk
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

// MySQL pool
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10
});

// Backblaze client (authorize lazily)
const b2 = new B2({
  accountId: process.env.B2_ACCOUNT_ID,
  applicationKey: process.env.B2_APPLICATION_KEY
});

let cachedDownloadUrl = null;
let lastAuthTime = 0;
const AUTH_TTL_MS = 5 * 60 * 1000;

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

// --- SSE store ---
// simple in-memory map of uploadId => response objects (SSE connections)
const sseClients = new Map();

// SSE endpoint: client connects here to receive progress events
app.get('/events/:uploadId', (req, res) => {
  const uploadId = req.params.uploadId;
  // set headers for SSE
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive'
  });
  // Node 18+ has res.flushHeaders; safe to call if available
  if (typeof res.flushHeaders === 'function') res.flushHeaders();

  // send an initial comment to keep connection alive
  res.write(`:ok\n\n`);

  // store client
  sseClients.set(uploadId, res);

  // cleanup when client disconnects
  req.on('close', () => {
    sseClients.delete(uploadId);
  });
});

// helper to send SSE events
function sendSse(uploadId, event, payload) {
  const res = sseClients.get(uploadId);
  if (!res) return;
  const data = JSON.stringify(payload);
  // SSE event format
  res.write(`event: ${event}\n`);
  // data lines: split into \n lines prefixed by "data: "
  data.split('\n').forEach(line => res.write(`data: ${line}\n`));
  res.write('\n'); // end of event
}

// Upload to Backblaze using axios + progress-stream
async function uploadStreamToBackblaze(uploadUrl, uploadAuthToken, filePath, destFileName, uploadId) {
  // get file size
  const stat = await fs.stat(filePath);
  const total = stat.size;

  // create read stream
  const readStream = fsSync.createReadStream(filePath);

  // progress-stream wrapper
  const prog = progress({ length: total, time: 100 }); // emits every 100ms
  readStream.pipe(prog);

  // Build axios request: stream as data, required headers:
  // - Authorization: uploadAuthToken
  // - X-Bz-File-Name: encoded file name (Backblaze expects raw fileName header)
  // - Content-Length
  // - X-Bz-Content-Sha1: do_not_verify (Backblaze allows this)
  const headers = {
    Authorization: uploadAuthToken,
    'X-Bz-File-Name': encodeURIComponent(destFileName),
    'Content-Length': total,
    'X-Bz-Content-Sha1': 'do_not_verify',
    'Content-Type': 'b2/x-auto' // generic; Backblaze doesn't require exact
  };

  // listen to progress and forward to SSE
  prog.on('progress', (p) => {
    const percent = Math.round(p.percentage);
    // payload: fileName, percent, loaded, total
    sendSse(uploadId, 'progress', { fileName: destFileName, percent, loaded: p.transferred, total });
  });

  // perform axios POST with the prog stream as body
  const axiosRes = await axios({
    method: 'post',
    url: uploadUrl,
    headers,
    data: prog,
    maxContentLength: Infinity,
    maxBodyLength: Infinity, // allow large uploads
    validateStatus: status => status >= 200 && status < 300 // only treat 2xx as success
  });

  return axiosRes.data; // backblaze upload response
}

// Upload helper that uses axios streaming and preserves retry-on-401 behaviour
async function uploadFileToB2WithProgress(localPath, destFileName, mimeType, uploadId) {
  await ensureAuthorized();

  const attempt = async () => {
    const { uploadUrl, uploadAuthToken } = await getFreshUploadUrl();
    try {
      // stream upload with progress
      const b2Res = await uploadStreamToBackblaze(uploadUrl, uploadAuthToken, localPath, destFileName, uploadId);
      // construct public url
      const fileUrl = `${cachedDownloadUrl}/file/${process.env.B2_BUCKET_NAME}/${encodeURIComponent(destFileName)}`;
      return {
        fileId: b2Res.fileId,
        fileName: destFileName,
        fileUrl,
        size: (await fs.stat(localPath)).size
      };
    } catch (err) {
      // axios will throw for non-2xx; check if it's 401
      const status = err?.response?.status;
      if (status === 401) throw { retryable401: true, inner: err };
      throw err;
    }
  };

  try {
    return await attempt();
  } catch (err) {
    if (err?.retryable401) {
      // re-authorize and retry once
      cachedDownloadUrl = null;
      await ensureAuthorized();
      return await attempt();
    }
    throw err;
  }
}

// endpoint: upload multiple files (client passes uploadId as query param)
app.post('/api/upload', upload.array('files', 1000), async (req, res) => {
  const uploadId = req.query.uploadId || uuidv4(); // clients can provide uploadId; otherwise server generates one

  if (!req.files || !req.files.length) return res.status(400).json({ message: 'No files uploaded', uploadId });

  const results = [];

  try {
    // process sequentially to keep progress predictable
    for (const file of req.files) {
      try {
        // notify client we started this file
        sendSse(uploadId, "started", {
          fileName: file.filename,
          originalName: file.originalname
        });

        const b2res = await uploadFileToB2WithProgress(file.path, file.filename, file.mimetype, uploadId);

        // save metadata to DB
        const [insertRes] = await pool.execute(
          `INSERT INTO uploads (original_name, b2_file_name, b2_file_url, size, mime_type)
           VALUES (?, ?, ?, ?, ?)`,
          [file.originalname, b2res.fileName, b2res.fileUrl, b2res.size, file.mimetype]
        );

        // delete temp file
        await fs.unlink(file.path).catch(()=>{});

        results.push({
          originalName: file.originalname,
          b2FileName: b2res.fileName,
          url: b2res.fileUrl,
          size: b2res.size,
          dbId: insertRes.insertId
        });

        // notify client that file finished
        sendSse(uploadId, 'done', { fileName: b2res.fileName, url: b2res.fileUrl });
      } catch (err) {
        // send error to client for this file
        console.error('Upload error for', file.originalname, err?.response?.data || err.message || err);
        sendSse(uploadId, 'error', { fileName: file.filename, error: (err?.response?.data || err.message || String(err)) });
        await fs.unlink(file.path).catch(()=>{});
        results.push({ originalName: file.originalname, error: err?.response?.data || err.message || String(err) });
      }
    }

    // all done - send final event (client may still listen)
    sendSse(uploadId, 'finished', { results });

    return res.json({ success: true, uploadId, results });
  } catch (err) {
    console.error('Unexpected error in upload route', err);
    sendSse(uploadId, 'error', { error: err?.response?.data || err.message || String(err) });
    return res.status(500).json({ success: false, message: err.message, uploadId });
  }
});

const port = process.env.PORT || 4000;
app.listen(port, () => console.log(`Server listening on ${port}`));
