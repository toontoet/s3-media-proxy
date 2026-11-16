# S3 Media Proxy

A proxy server for serving media (images and video) from S3-compatible object storage. Supports multiple storage backends and range requests for efficient streaming of large files.

## Features

- ✅ Multiple configurable S3 backends
- ✅ Range request support (video streaming)
- ✅ Optimized for large files
- ✅ Access logging and error handling
- ✅ Health check endpoint
- ✅ Docker support
- ✅ Node.js 22

## Requirements

- Node.js 22 or higher
- Docker (optional, for containerized deployment)

## Configuration

Create a `storage.json` file with your S3 backend configurations:

```json
{
  "MAIN": {
    "type": "s3",
    "accessKey": "YOUR_ACCESS_KEY",
    "accessSecret": "YOUR_SECRET_KEY",
    "region": "eu-central",
    "endpoint": "https://fsn1.your-objectstorage.com",
    "bucket": "your-bucket-name",
    "folder": "media",
    "healthCheckKey": "healthcheck.txt"
  },
  "VODSERVER": {
    "type": "s3",
    "accessKey": "YOUR_ACCESS_KEY",
    "accessSecret": "YOUR_SECRET_KEY",
    "region": "eu-central",
    "endpoint": "https://fsn1.your-objectstorage.com",
    "bucket": "your-vod-bucket",
    "folder": "ondemand",
    "healthCheckKey": "healthcheck.txt"
  }
}
```

### Configuration Options

- `type`: Must be `"s3"`
- `accessKey`: S3 access key
- `accessSecret`: S3 secret key
- `region`: AWS region
- `endpoint`: S3-compatible endpoint URL (with `https://` or `http://`; host-only values still work)
- `bucket`: Bucket name
- `folder`: Folder prefix within the bucket (optional)
- `healthCheckKey`: Object key used by `/ready` to verify object access (optional, relative to `folder`)

## Installation and Usage

### Running Locally

```bash
# Install dependencies
npm install

# Start the server
npm start

# Or with auto-reload during development
npm run dev
```

The server runs on port 3000 by default.

### Docker

#### Building the Image

```bash
docker build -t s3-media-proxy .
```

#### Starting the Container

```bash
docker run -d \
  --name s3-media-proxy \
  -p 3000:3000 \
  -v $(pwd)/storage.json:/config/storage.json:ro \
  s3-media-proxy
```

#### Docker Compose (Recommended)

Create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  s3-media-proxy:
    build: .
    ports:
      - "3000:3000"
    volumes:
      - ./storage.json:/config/storage.json:ro
    restart: unless-stopped
    environment:
      - NODE_ENV=production
      - READINESS_TIMEOUT_MS=5000
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:3000/ready"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

Start with:

```bash
docker-compose up -d
```

## API Endpoints

### Health Check

```
GET /health
```

Returns the server liveness status and available backends. This only confirms that the Node.js process can answer requests.

**Response:**
```json
{
  "status": "ok",
  "backends": ["MAIN", "VODSERVER"],
  "timestamp": "2025-10-29T12:00:00.000Z"
}
```

### Readiness Check

```
GET /ready
```

Checks whether each configured storage backend is reachable. If `healthCheckKey` is configured, the proxy verifies that object with `HeadObject`; otherwise it falls back to `HeadBucket`.

The endpoint returns `200` when all backends are ready and `503` when one or more backends fail. Docker Compose uses this endpoint for container health so storage or credential failures are detected instead of only checking that Express is alive.

### Media Proxy

```
GET /api/storage/{backend}/{filepath}
```

Serves a file via the specified storage backend.

**Parameters:**
- `backend`: Name of the backend as defined in `storage.json`
- `filepath`: Path to the file (may contain slashes for subfolders)

**Examples:**

```bash
# Get image from MAIN backend
http://localhost:3000/api/storage/MAIN/images/photo.jpg

# Get video from VODSERVER backend (in subfolder)
http://localhost:3000/api/storage/VODSERVER/movies/2024/trailer.mp4
```

### Folder Prefix

The `folder` setting from the storage config is automatically prefixed:

- Config: `"folder": "media"`
- Request: `/api/storage/MAIN/images/photo.jpg`
- S3 key: `media/images/photo.jpg`

## Range Requests

The proxy automatically supports HTTP range requests, which is essential for:
- Video streaming (seeking in videos)
- Large files (resumable downloads)
- Bandwidth optimization

## Logging

### Standard Logging

The application logs all requests concisely to the console:

```
✓ Initialized backend: MAIN (openrotterdam/media)
✓ S3 Media Proxy listening on port 3000
✓ Backends: MAIN, VODSERVER
✓ 2025-10-29T12:00:00.000Z | GET | /api/storage/MAIN/video.mp4 | 200 | 45ms | Mozilla/5.0...
✗ 2025-10-29T12:00:05.000Z | GET | /api/storage/MAIN/missing.jpg | 404 | 12ms | curl/7.64.1
```

- ✓ = Successful operations
- ✗ = Errors

### Debug Logging

Set `DEBUG=true` for verbose logging including:
- S3 configuration details at startup
- Full request details (backend, path, range)
- Stream progress (bytes transferred)
- Detailed error information

## Environment Variables

- `PORT`: Server port (default: 3000)
- `STORAGE_CONFIG_PATH`: Path to storage.json (default: ./storage.json)
- `NODE_ENV`: Node environment (production/development)
- `DEBUG`: Verbose debug logging and debug endpoints (default: false)
- `READINESS_TIMEOUT_MS`: Per-backend readiness timeout in milliseconds (default: 5000)

### Debug Mode

Set `DEBUG=true` to enable additional logging and debug endpoints:

```bash
# Local
DEBUG=true npm start

# Docker Compose
services:
  s3-media-proxy:
    environment:
      - DEBUG=true
```

Debug endpoints (only available with `DEBUG=true`):
- `GET /debug/check/{backend}/{filepath}` - Check if a file exists
- `GET /debug/list/{backend}/{prefix}` - List files in a folder

## Troubleshooting

### "Failed to load storage configuration"

Ensure `storage.json` exists and is valid JSON. For Docker: verify the volume mount is correct.

### "Storage backend not found"

The specified backend name does not exist in `storage.json`. Use the `/health` endpoint to see available backends.

### "File not found"

The file does not exist in the S3 bucket. Check:
- If the folder prefix is correct
- If the file actually exists in the bucket
- If the S3 credentials are correct

### Connection errors

Verify:
- S3 endpoint is reachable
- Access key and secret are correct
- Bucket name is correct
- Firewall/network settings

### "/ready" returns 503

The process is alive, but at least one storage backend cannot be verified. Check the response body and logs for the backend name, HTTP status, and error message.

If credentials only allow object reads and not bucket-level checks, add a small object such as `healthcheck.txt` to the configured folder and set `healthCheckKey` for that backend.

Configuration and credentials are loaded once at startup. Restart the container after changing `storage.json` or rotating credentials.

## Security

⚠️ **Important**: 
- Do NOT store `storage.json` in version control (it's in .gitignore)
- Use read-only credentials when possible
- Consider rate limiting for production use
- Use HTTPS in production

## Performance

The application is optimized for:
- Large files (streaming)
- Range requests (partial content)
- Concurrent requests (Node.js async I/O)
- Memory efficiency (no caching, direct streaming)

## License

ISC
