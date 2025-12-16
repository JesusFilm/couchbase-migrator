FROM --platform=linux/amd64 mcr.microsoft.com/devcontainers/javascript-node:1-20-bookworm

# Install pnpm
RUN npm install -g pnpm@10.15.1

# Set working directory
WORKDIR /workspace

# Copy .env file if it exists (wildcard pattern prevents build failure if .env doesn't exist)
# The workspace will be mounted at runtime, but this ensures .env is available in the image
COPY .env* ./

# Keep container running
CMD ["sleep", "infinity"]

