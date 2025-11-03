import express from 'express';
import { S3Client } from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import http from 'http';
import https from 'https';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const DEBUG = process.env.DEBUG === 'true';

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
    if (config.type === 's3') {
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
          endpoint: config.endpoint ? `https://${config.endpoint}` : undefined,
          credentials: {
            accessKeyId: config.accessKey,
            secretAccessKey: config.accessSecret,
          },
          forcePathStyle: true, // Required for S3-compatible services
          requestHandler: httpHandler,
        });
        
        proxyInstances[name] = { s3: s3Client, config };
        
        console.log(`✓ Initialized backend: ${name} (${config.bucket}/${config.folder || ''})`);
      } catch (error) {
        console.error(`✗ Failed to initialize S3 client for ${name}:`, error.message);
        console.error(error);
        process.exit(1);
      }
    }
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

// Debug endpoints (only enabled with DEBUG=true)
if (DEBUG) {
  
app.get('/debug/list/:configName/*?', async (req, res) => {
  const { configName } = req.params;
  const prefix = req.params[0] || '';
  
  if (!proxyInstances[configName]) {
    return res.status(404).json({ error: 'Backend not found' });
  }
  
  const { s3: s3Client, config } = proxyInstances[configName];
  const fullPrefix = config.folder ? `${config.folder}/${prefix}` : prefix;
  
  try {
    const { ListObjectsV2Command } = await import('@aws-sdk/client-s3');
    
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
  
  const { s3: s3Client, config } = proxyInstances[configName];
  const s3Key = config.folder ? `${config.folder}/${filePath}` : filePath;
  
  try {
    const { HeadObjectCommand } = await import('@aws-sdk/client-s3');
    
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

  const { s3: s3Client, config } = proxyInstances[configName];

  // Construct full S3 key with folder prefix
  const s3Key = config.folder ? `${config.folder}/${filePath}` : filePath;

  if (DEBUG) {
    console.log(`🔍 ${configName}/${filePath} (range: ${req.headers.range || 'full'})`);
  }

  try {
    const { GetObjectCommand } = await import('@aws-sdk/client-s3');
    
    // Get object from S3 (with range support)
    const command = new GetObjectCommand({
      Bucket: config.bucket,
      Key: s3Key,
      Range: req.headers.range,
    });
    
    const response = await s3Client.send(command);
    
    // Set response headers
    if (req.headers.range) {
      res.status(206); // Partial Content
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
    
    // Stream the body to response
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
    
    response.Body.pipe(res);
    
  } catch (error) {
    console.error(`✗ ${configName}/${filePath}: ${error.message}`);
    
    if (DEBUG) {
      console.error(`   - Error type: ${error.constructor.name}`);
      console.error(`   - Status: ${error.$metadata?.httpStatusCode || 'unknown'}`);
    }
    
    // Only send error response if headers haven't been sent yet
    if (!res.headersSent) {
      if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
        return res.status(404).json({
          error: 'File not found',
          path: filePath,
          backend: configName
        });
      }
      
      return res.status(500).json({
        error: 'Internal server error',
        message: error.message
      });
    }
  }
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
app.listen(PORT, () => {
  console.log(`✓ S3 Media Proxy listening on port ${PORT}`);
  console.log(`✓ Backends: ${Object.keys(proxyInstances).join(', ')}`);
  if (DEBUG) {
    console.log(`✓ Debug mode enabled - /debug/check and /debug/list available`);
  }
});

