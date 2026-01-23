# 🏛️ Doctore Dashboard

> *"Train your data for the migration arena"*

Real-time database migration control panel for the Doctore suite.

## Quick Start

### Prerequisites

```bash
# Install Rust WASM target
rustup target add wasm32-unknown-unknown

# Install Trunk (WASM bundler)
cargo install trunk
```

### Development (Mock Mode)

```bash
cd services/doctore-dash

# Run development server with hot reload
trunk serve

# Opens http://localhost:3000 automatically
```

### Docker

```bash
cd services/doctore-dash

# Build and run
docker-compose up --build

# Access at http://localhost:3000
```

## Features

- **Real-time Progress** — Live migration status with donut chart
- **Throughput Graph** — Rolling 60-second throughput visualization
- **Table Progress** — Per-table migration status
- **Filter Stats** — Tenant/table filtering statistics
- **Event Log** — Live event stream
- **Controls** — Start/Pause/Stop/Config

## Color Palette

| Color | Hex | Use |
|-------|-----|-----|
| Crimson | `#DC143C` | Primary accent |
| Blood Red | `#8B0000` | Hover states |
| Gold | `#FFD700` | Success, highlights |
| Silver | `#C0C0C0` | Secondary elements |
| Charcoal | `#36454F` | Dark backgrounds |

## Architecture

```
doctore-dash/
├── src/
│   ├── main.rs           # Entry point
│   ├── app.rs            # Main app component
│   ├── state.rs          # Reactive state
│   ├── mock.rs           # Mock data generator
│   └── components/
│       ├── header.rs     # Header with service status
│       ├── progress.rs   # Progress donut
│       ├── throughput.rs # Throughput chart
│       ├── stats.rs      # Stats cards
│       ├── tables.rs     # Table progress list
│       ├── filter.rs     # Filter statistics
│       ├── controls.rs   # Control buttons
│       └── log.rs        # Event log
├── style/
│   └── main.css          # Roman gladiator theme
├── public/
│   └── index.html        # HTML entry
├── Cargo.toml
├── Trunk.toml            # WASM build config
└── docker-compose.yml
```

## Connecting to Live Services

```bash
# Start with WebSocket URL to live Doctore API
./doctore-dash --ws-url ws://doctore-api:8080/ws
```

## Stack

- **Leptos** — Rust reactive UI framework
- **WASM** — WebAssembly for browser
- **Charming** — Rust charting (D3.js equivalent)
- **Tokio** — Async WebSocket client

---

**flarestick.io** | High-performance post-production technology
