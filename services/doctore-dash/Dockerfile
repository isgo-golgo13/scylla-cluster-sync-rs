# services/doctore-dash/Dockerfile
#
# Doctore Dashboard - Multi-stage WASM build
#

# Stage 1: Build WASM
FROM rust:1.75-slim as builder

# Install wasm target and trunk
RUN rustup target add wasm32-unknown-unknown
RUN cargo install trunk wasm-bindgen-cli

# Set working directory
WORKDIR /app

# Copy manifests first (better caching)
COPY Cargo.toml ./

# Create dummy src for dependency caching
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && echo "pub fn hydrate() {}" > src/lib.rs
RUN cargo build --release --target wasm32-unknown-unknown || true

# Copy actual source
COPY src ./src
COPY style ./style
COPY public ./public

# Build with trunk
RUN trunk build --release

# Stage 2: Serve with lightweight server
FROM nginx:alpine

# Copy built assets
COPY --from=builder /app/dist /usr/share/nginx/html

# Copy nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]
