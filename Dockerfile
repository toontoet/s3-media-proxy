# Use Node.js 22 Alpine for smaller image size
FROM node:22-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install production dependencies
RUN npm install --omit=dev

# Copy application code
COPY index.js ./

# Create directory for mounted storage config
RUN mkdir -p /config

# Set environment variable for config path
ENV STORAGE_CONFIG_PATH=/config/storage.json

# Expose port
EXPOSE 3000

# Run as non-root user for security
USER node

# Start the application
CMD ["node", "index.js"]

