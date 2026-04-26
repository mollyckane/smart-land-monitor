# Smart Land Health Monitoring Network
### Distributed Systems CA — HDip in Science in Computing (Software Development) | National College of Ireland
### SDG 15: Life on Land 

---

## Project Overview

This application simulates a distributed network of smart environmental sensors
deployed across a terrestrial ecosystem to monitor conditions, detect threats
and support the United Nations Sustainable Development Goal 15: Life on Land.

The system consists of three gRPC services that register with a central naming
service, discover one another, and communicate in real time. A graphical client
acts as a mission control dashboard to control and monitor all services.

---

## Technologies Used

- **Node.js** — Runtime environment
- **gRPC (`@grpc/grpc-js`)** — Service communication
- **Protocol Buffers (`@grpc/proto-loader`)** — Service definitions
- **npm** — Package management

---

## Project Structure
```
smart-land-monitor/
├── .gitignore
├── client/
│   └── guiClient.js
├── naming/
│   └── namingService.js
├── package-lock.json
├── package.json
├── protos/
│   ├── forest.proto
│   ├── naming.proto
│   ├── soil.proto
│   └── water.proto
├── README.md
└── services/
    ├── forestService.js
    ├── soilService.js
    └── waterService.js
```

---

##  Services

### 1. Forest Air Quality & Humidity Monitor (`port 50052`)
Monitors forest canopy conditions including humidity, CO₂, and oxygen levels.
Supports **Simple RPC**, **Server Streaming**, and **Bidirectional Streaming**.

### 2. Soil Degradation & Desertification Sensor (`port 50053`)
Measures soil moisture and erosion risk to combat desertification.
Supports **Simple RPC**, **Client Streaming**, and **Server Streaming**.

### 3. Freshwater Source & Pollution Tracker (`port 50054`)
Monitors mountain freshwater sources for pollution and water quality.
Supports **Simple RPC**, **Client Streaming**, **Server Streaming**, and **Bidirectional Streaming**.

### Naming Service (`port 50051`)
Central registry where all services register on startup and are discovered
by the client and other services.

---

### Prerequisites

- Node.js v18 or higher
- npm

### Installation

```bash
git clone https://github.com/mollyckane/smart-land-monitor.git
cd smart-land-monitor
npm install
```

### Running the Application

Open a **separate terminal for each** of the following:

```bash
# 1. Start the Naming Service first
npm run naming
# or 
node naming/namingService.js

# 2. Start the Forest Monitor
npm run forest
# or 
node services/forestService.js

# 3. Start the Soil Sensor
npm run soil
# or 
node services/soilService.js

# 4. Start the Freshwater Tracker
npm run water
# or 
node services/waterService.js

# 5. Launch the GUI  Web Client
npm run web-client
# or 
node web-client/bin/www
```

---

##  gRPC RPC Types Used

| RPC Style | Used In |
|---|---|
| Simple RPC | All 3 services |
| Server Streaming | Forest Monitor, Soil Sensor, Water Tracker |
| Client Streaming | Soil Sensor, Water Tracker |
| Bidirectional Streaming | Forest Monitor, Water Tracker |

---

## Features
- Service registration and discovery via Naming Service
- All 4 gRPC communication styles
- Remote error handling and deadline management
- GUI client for service discovery and control
- Real-time data streaming

---

## UN SDG 15 — Life on Land

This application directly supports SDG 15 by:
- Monitoring forest health indicators to detect early signs of biodiversity stress
- Tracking soil moisture and erosion risk to combat desertification
- Protecting freshwater watersheds that supply 75% of the world's freshwater

---


---

## Author

- **Name:** Molly Kane
- **Student ID:** 25132539
- **Module:** Distributed Systems
- **Institution:** National College of Ireland
