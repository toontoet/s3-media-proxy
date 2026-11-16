import express from 'express';
import {
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import http from 'http';
import https from 'https';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { Readable } from 'stream';
import { pipeline } from 'stream/promises';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const DEBUG = process.env.DEBUG === 'true';
const READINESS_TIMEOUT_MS = parsePositiveInteger(process.env.READINESS_TIMEOUT_MS, 5000);

function parsePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function buildS3Key(config, filePath) {
  return config.folder ? `${config.folder}/${filePath}` : filePath;
}

function normalizeS3Endpoint(endpoint) {
  if (!endpoint) {
    return undefined;
  }

  if (/^https?:\/\//i.test(endpoint)) {
    return endpoint;
  }

  return `https://${endpoint}`;
}

function getBackendType(config) {
  if (config.type === 's3') {
    return 's3';
  }

  if (config.url) {
    return 'http';
  }

  return null;
}

function buildHttpUrl(config, filePath) {
  const base = config.url.endsWith('/') ? config.url : `${config.url}/`;
  const path = filePath.startsWith('/') ? filePath.slice(1) : filePath;
  return new URL(path, base).href;
}

function buildHttpAuthHeader(config) {
  if (!config.username) {
    return undefined;
  }

  const credentials = Buffer.from(`${config.username}:${config.password ?? ''}`).toString('base64');
  return `Basic ${credentials}`;
}

function isClientAbortError(error) {
  return (
    error?.name === 'AbortError' ||
    error?.name === 'RequestAbortedError' ||
    error?.code === 'ABORT_ERR' ||
    error?.code === 'ERR_STREAM_PREMATURE_CLOSE'
  );
}

// Load storage configuration
let storageConfig;
try {
  const configPath = process.env.STORAGE_CONFIG_PATH || join(__dirname, 'storage.json');
  storageConfig = JSON.parse(readFileSync(configPath, 'utf8'));
  console.log(`✓ Loaded storage configuration from: ${configPath}`);
  console.log(`✓ Available storage backends: ${Object.keys(storageConfig).join(', ')}`);
} catch (error) {
  console.error('✗ Failed to load storage configuration:', error.message);
  process.exit(1);
}

// Initialize S3 proxy instances for each backend
const proxyInstances = {};

async function initializeProxies() {
  for (const [name, config] of Object.entries(storageConfig)) {
    const backendType = getBackendType(config);

    if (backendType === 's3') {
      try {
        if (DEBUG) {
          console.log(`🔧 Initializing S3 client for: ${name}`);
          console.log(`   - Bucket: ${config.bucket}`);
          console.log(`   - Region: ${config.region}`);
          console.log(`   - Endpoint: ${config.endpoint || 'default'}`);
          console.log(`   - Folder prefix: ${config.folder || 'none'}`);
        }

        // Configure HTTP agents with higher socket limits for high concurrency
        // This prevents "socket usage at capacity" warnings under load
        const httpAgent = new http.Agent({
          keepAlive: true,
          keepAliveMsecs: 1000,
          maxSockets: 200,
          maxFreeSockets: 50,
        });

        const httpsAgent = new https.Agent({
          keepAlive: true,
          keepAliveMsecs: 1000,
          maxSockets: 200,
          maxFreeSockets: 50,
        });

        // Configure HTTP handler with custom agents
        const httpHandler = new NodeHttpHandler({
          requestTimeout: 30000,
          connectionTimeout: 30000,
          socketAcquisitionWarningTimeout: 10000,
          maxAttempts: 3,
          httpAgent,
          httpsAgent,
        });

        const s3Client = new S3Client({
          region: config.region,
          endpoint: normalizeS3Endpoint(config.endpoint),
          credentials: {
            accessKeyId: config.accessKey,
            secretAccessKey: config.accessSecret,
          },
          forcePathStyle: true, // Required for S3-compatible services
          requestHandler: httpHandler,
        });

        proxyInstances[name] = { type: 's3', s3: s3Client, config };

        console.log(`✓ Initialized backend: ${name} (s3://${config.bucket}/${config.folder || ''})`);
      } catch (error) {
        console.error(`✗ Failed to initialize S3 client for ${name}:`, error.message);
        console.error(error);
        process.exit(1);
      }
      continue;
    }

    if (backendType === 'http') {
      try {
        if (DEBUG) {
          console.log(`🔧 Initializing HTTP backend for: ${name}`);
          console.log(`   - URL: ${config.url}`);
          console.log(`   - Auth: ${config.username ? 'basic' : 'none'}`);
        }

        proxyInstances[name] = { type: 'http', config };
        console.log(`✓ Initialized backend: ${name} (${config.url})`);
      } catch (error) {
        console.error(`✗ Failed to initialize HTTP backend for ${name}:`, error.message);
        console.error(error);
        process.exit(1);
      }
      continue;
    }

    console.error(`✗ Unknown storage backend configuration for ${name}: expected type "s3" or a "url" field`);
    process.exit(1);
  }
}

await initializeProxies();

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    backends: Object.keys(proxyInstances),
    timestamp: new Date().toISOString()
  });
});

async function checkBackendReadiness(name, proxy) {
  const startedAt = Date.now();

  if (proxy.type === 'http') {
    const { config } = proxy;
    const checkUrl = config.healthCheckPath
      ? buildHttpUrl(config, config.healthCheckPath)
      : config.url;

    try {
      const headers = {};
      const authorization = buildHttpAuthHeader(config);
      if (authorization) {
        headers.Authorization = authorization;
      }

      const response = await fetch(checkUrl, {
        method: 'HEAD',
        headers,
        signal: AbortSignal.timeout(READINESS_TIMEOUT_MS),
      });

      if (!response.ok && response.status !== 405) {
        throw new Error(`HTTP ${response.status}`);
      }

      return {
        name,
        status: 'ok',
        type: 'http',
        url: config.url,
        check: config.healthCheckPath ? 'path' : 'url',
        durationMs: Date.now() - startedAt,
      };
    } catch (error) {
      const result = {
        name,
        status: 'error',
        type: 'http',
        url: config.url,
        check: config.healthCheckPath ? 'path' : 'url',
        durationMs: Date.now() - startedAt,
        error: error.message,
        errorType: error.name,
      };

      console.error(`✗ Readiness failed for ${name}: ${error.message}`);
      return result;
    }
  }

  const { s3: s3Client, config } = proxy;
  const checkType = config.healthCheckKey ? 'object' : 'bucket';

  try {
    const command = config.healthCheckKey
      ? new HeadObjectCommand({
          Bucket: config.bucket,
          Key: buildS3Key(config, config.healthCheckKey),
        })
      : new HeadBucketCommand({
          Bucket: config.bucket,
        });

    await s3Client.send(command, {
      abortSignal: AbortSignal.timeout(READINESS_TIMEOUT_MS),
    });

    return {
      name,
      status: 'ok',
      type: 's3',
      bucket: config.bucket,
      check: checkType,
      durationMs: Date.now() - startedAt,
    };
  } catch (error) {
    const result = {
      name,
      status: 'error',
      type: 's3',
      bucket: config.bucket,
      check: checkType,
      durationMs: Date.now() - startedAt,
      error: error.message,
      errorType: error.name,
      httpStatus: error.$metadata?.httpStatusCode,
    };

    console.error(`✗ Readiness failed for ${name}: ${error.message}`);
    return result;
  }
}

// Readiness check endpoint: validates that configured storage backends are reachable.
app.get('/ready', async (req, res) => {
  const checks = await Promise.all(
    Object.entries(proxyInstances).map(([name, proxy]) => checkBackendReadiness(name, proxy))
  );
  const isReady = checks.every((check) => check.status === 'ok');

  res.status(isReady ? 200 : 503).json({
    status: isReady ? 'ok' : 'degraded',
    backends: checks,
    timeoutMs: READINESS_TIMEOUT_MS,
    timestamp: new Date().toISOString(),
  });
});

// Debug endpoints (only enabled with DEBUG=true)
if (DEBUG) {
  
app.get('/debug/list/:configName/*?', async (req, res) => {
  const { configName } = req.params;
  const prefix = req.params[0] || '';
  
  if (!proxyInstances[configName]) {
    return res.status(404).json({ error: 'Backend not found' });
  }

  const proxy = proxyInstances[configName];
  if (proxy.type !== 's3') {
    return res.status(400).json({ error: 'Debug listing is only supported for S3 backends' });
  }
  
  const { s3: s3Client, config } = proxy;
  const fullPrefix = config.folder ? `${config.folder}/${prefix}` : prefix;
  
  try {
    const command = new ListObjectsV2Command({
      Bucket: config.bucket,
      Prefix: fullPrefix,
      MaxKeys: 100,
    });
    
    const response = await s3Client.send(command);
    
    res.json({
      backend: configName,
      bucket: config.bucket,
      prefix: fullPrefix,
      files: response.Contents?.map(item => ({
        key: item.Key,
        size: item.Size,
        lastModified: item.LastModified,
      })) || []
    });
  } catch (error) {
    res.status(500).json({
      error: 'Failed to list files',
      message: error.message
    });
  }
});

// Debug endpoint to check if a specific file exists
app.get('/debug/check/:configName/*', async (req, res) => {
  const { configName } = req.params;
  const filePath = req.params[0];
  
  if (!proxyInstances[configName]) {
    return res.status(404).json({ error: 'Backend not found' });
  }

  const proxy = proxyInstances[configName];
  if (proxy.type !== 's3') {
    return res.status(400).json({ error: 'Debug checks are only supported for S3 backends' });
  }
  
  const { s3: s3Client, config } = proxy;
  const s3Key = config.folder ? `${config.folder}/${filePath}` : filePath;
  
  try {
    const command = new HeadObjectCommand({
      Bucket: config.bucket,
      Key: s3Key,
    });
    
    const response = await s3Client.send(command);
    
    res.json({
      exists: true,
      backend: configName,
      bucket: config.bucket,
      key: s3Key,
      size: response.ContentLength,
      contentType: response.ContentType,
      lastModified: response.LastModified,
    });
  } catch (error) {
    if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
      return res.json({
        exists: false,
        backend: configName,
        bucket: config.bucket,
        key: s3Key,
        error: 'File not found'
      });
    }
    
    res.status(500).json({
      error: 'Failed to check file',
      message: error.message
    });
  }
});

}  // End DEBUG endpoints

// Logging middleware
app.use((req, res, next) => {
  const startTime = Date.now();
  
  // Log response when finished
  res.on('finish', () => {
    const duration = Date.now() - startTime;
    const logLine = [
      new Date().toISOString(),
      req.method,
      req.originalUrl,
      res.statusCode,
      `${duration}ms`,
      req.get('user-agent') || '-'
    ].join(' | ');
    
    if (res.statusCode >= 400) {
      console.error(`✗ ${logLine}`);
    } else {
      console.log(`✓ ${logLine}`);
    }
  });
  
  next();
});

function createAbortHandlers(configName, filePath, abortController, getUpstreamBody) {
  let clientAborted = false;

  const abortUpstream = (reason) => {
    if (clientAborted) {
      return;
    }

    clientAborted = true;
    console.warn(`⚠ Client aborted ${configName}/${filePath}: ${reason}`);

    if (!abortController.signal.aborted) {
      abortController.abort();
    }

    const upstreamBody = getUpstreamBody();
    if (upstreamBody && typeof upstreamBody.destroy === 'function') {
      upstreamBody.destroy(new Error(reason));
    }
  };

  const bindResponseCloseHandler = (res) => () => {
    if (!res.writableEnded) {
      abortUpstream('response closed before stream completed');
    }
  };

  return {
    clientAborted: () => clientAborted,
    onRequestAborted: () => abortUpstream('request aborted'),
    bindResponseCloseHandler,
  };
}

async function serveS3Object(proxy, configName, filePath, req, res) {
  const { s3: s3Client, config } = proxy;
  const s3Key = buildS3Key(config, filePath);

  if (DEBUG) {
    console.log(`🔍 ${configName}/${filePath} (s3, range: ${req.headers.range || 'full'})`);
  }

  let response;
  const abortController = new AbortController();
  const { clientAborted, onRequestAborted, bindResponseCloseHandler } = createAbortHandlers(
    configName,
    filePath,
    abortController,
    () => response?.Body
  );
  const onResponseClosed = bindResponseCloseHandler(res);

  req.on('aborted', onRequestAborted);
  res.on('close', onResponseClosed);

  try {
    const command = new GetObjectCommand({
      Bucket: config.bucket,
      Key: s3Key,
      Range: req.headers.range,
    });

    response = await s3Client.send(command, {
      abortSignal: abortController.signal,
    });

    if (req.headers.range) {
      res.status(206);
      res.setHeader('Content-Range', response.ContentRange);
    } else {
      res.status(200);
    }

    res.setHeader('Content-Type', response.ContentType || 'application/octet-stream');
    res.setHeader('Content-Length', response.ContentLength);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'public, max-age=31536000');
    res.setHeader('Last-Modified', response.LastModified?.toUTCString() || new Date().toUTCString());

    if (response.ETag) {
      res.setHeader('ETag', response.ETag);
    }

    if (DEBUG) {
      let bytesStreamed = 0;
      response.Body.on('data', (chunk) => {
        bytesStreamed += chunk.length;
      });
      response.Body.on('end', () => {
        console.log(`   ✓ Streamed ${bytesStreamed} bytes`);
      });
      response.Body.on('error', (err) => {
        console.error(`   ✗ Stream error:`, err.message);
      });
    }

    await pipeline(response.Body, res);
  } catch (error) {
    if (clientAborted() || isClientAbortError(error)) {
      if (DEBUG) {
        console.warn(`⚠ Stream aborted for ${configName}/${filePath}: ${error.message}`);
      }
      return;
    }

    console.error(`✗ ${configName}/${filePath}: ${error.message}`);

    if (DEBUG) {
      console.error(`   - Error type: ${error.constructor.name}`);
      console.error(`   - Status: ${error.$metadata?.httpStatusCode || 'unknown'}`);
    }

    if (!res.headersSent) {
      if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
        return res.status(404).json({
          error: 'File not found',
          path: filePath,
          backend: configName,
        });
      }

      return res.status(500).json({
        error: 'Internal server error',
        message: error.message,
      });
    }
  } finally {
    req.off('aborted', onRequestAborted);
    res.off('close', onResponseClosed);
  }
}

async function serveHttpObject(proxy, configName, filePath, req, res) {
  const { config } = proxy;
  const targetUrl = buildHttpUrl(config, filePath);

  if (DEBUG) {
    console.log(`🔍 ${configName}/${filePath} (http, range: ${req.headers.range || 'full'})`);
    console.log(`   - Target URL: ${targetUrl}`);
  }

  let upstreamBody;
  const abortController = new AbortController();
  const { clientAborted, onRequestAborted, bindResponseCloseHandler } = createAbortHandlers(
    configName,
    filePath,
    abortController,
    () => upstreamBody
  );
  const onResponseClosed = bindResponseCloseHandler(res);

  req.on('aborted', onRequestAborted);
  res.on('close', onResponseClosed);

  try {
    const headers = {};
    const authorization = buildHttpAuthHeader(config);
    if (authorization) {
      headers.Authorization = authorization;
    }
    if (req.headers.range) {
      headers.Range = req.headers.range;
    }

    const response = await fetch(targetUrl, {
      method: 'GET',
      headers,
      signal: abortController.signal,
    });

    if (!response.ok && response.status !== 206) {
      if (response.status === 404) {
        return res.status(404).json({
          error: 'File not found',
          path: filePath,
          backend: configName,
        });
      }

      return res.status(response.status).json({
        error: 'Upstream request failed',
        status: response.status,
        path: filePath,
        backend: configName,
      });
    }

    res.status(response.status);

    const passthroughHeaders = [
      'content-type',
      'content-length',
      'content-range',
      'accept-ranges',
      'last-modified',
      'etag',
    ];

    for (const headerName of passthroughHeaders) {
      const value = response.headers.get(headerName);
      if (value) {
        res.setHeader(headerName, value);
      }
    }

    if (!response.headers.has('accept-ranges')) {
      res.setHeader('Accept-Ranges', 'bytes');
    }

    if (!response.headers.has('cache-control')) {
      res.setHeader('Cache-Control', 'public, max-age=31536000');
    }

    if (!response.body) {
      return res.end();
    }

    upstreamBody = Readable.fromWeb(response.body);

    if (DEBUG) {
      let bytesStreamed = 0;
      upstreamBody.on('data', (chunk) => {
        bytesStreamed += chunk.length;
      });
      upstreamBody.on('end', () => {
        console.log(`   ✓ Streamed ${bytesStreamed} bytes`);
      });
      upstreamBody.on('error', (err) => {
        console.error(`   ✗ Stream error:`, err.message);
      });
    }

    await pipeline(upstreamBody, res);
  } catch (error) {
    if (clientAborted() || isClientAbortError(error)) {
      if (DEBUG) {
        console.warn(`⚠ Stream aborted for ${configName}/${filePath}: ${error.message}`);
      }
      return;
    }

    console.error(`✗ ${configName}/${filePath}: ${error.message}`);

    if (!res.headersSent) {
      return res.status(500).json({
        error: 'Internal server error',
        message: error.message,
      });
    }
  } finally {
    req.off('aborted', onRequestAborted);
    res.off('close', onResponseClosed);
  }
}

// Main API endpoint: /api/storage/:configName/*
app.get('/api/storage/:configName/*', async (req, res, next) => {
  const { configName } = req.params;
  const filePath = req.params[0]; // Everything after configName

  // Validate config exists
  if (!proxyInstances[configName]) {
    console.error(`✗ Invalid storage backend requested: ${configName}`);
    return res.status(404).json({
      error: 'Storage backend not found',
      backend: configName,
      available: Object.keys(proxyInstances)
    });
  }

  // Validate file path
  if (!filePath) {
    console.error(`✗ No file path provided for backend: ${configName}`);
    return res.status(400).json({
      error: 'File path is required'
    });
  }

  const proxy = proxyInstances[configName];

  if (proxy.type === 'http') {
    return serveHttpObject(proxy, configName, filePath, req, res);
  }

  return serveS3Object(proxy, configName, filePath, req, res);
});

// 404 handler
app.use((req, res) => {
  if (DEBUG) {
    console.error(`✗ Route not found: ${req.method} ${req.originalUrl}`);
  }
  res.status(404).json({
    error: 'Not found',
    path: req.originalUrl
  });
});

// Error handler
app.use((error, req, res, next) => {
  console.error('✗ Unhandled error:', error);
  
  if (!res.headersSent) {
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// Start server
const server = app.listen(PORT, () => {
  console.log(`✓ S3 Media Proxy listening on port ${PORT}`);
  console.log(`✓ Backends: ${Object.keys(proxyInstances).join(', ')}`);
  if (DEBUG) {
    console.log(`✓ Debug mode enabled - /debug/check and /debug/list available`);
  }
});

function shutdown(signal) {
  console.log(`✓ Received ${signal}, shutting down gracefully`);

  server.close(() => {
    for (const proxy of Object.values(proxyInstances)) {
      if (proxy.type === 's3') {
        proxy.s3.destroy();
      }
    }

    console.log('✓ Shutdown complete');
    process.exit(0);
  });

  setTimeout(() => {
    console.error('✗ Graceful shutdown timed out');
    process.exit(1);
  }, 10000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

