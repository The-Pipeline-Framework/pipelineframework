# CSV Payments Processing Dashboard

A React-based dashboard for visualizing the CSV Payments Processing pipeline in real-time.

## Features

- Drag & drop file upload with upload speed gauge
- Real-time processing metrics for each pipeline step:
  - Input CSV File Processing Service (Records/second)
  - Payments Processing Service (PaymentStatus/second)
  - Payment Status Service (PaymentStatus/second)
  - Output CSV File Processing Service (PaymentOutput/second)
- Visual pipeline representation
- Responsive design that works on desktop and mobile

## Architecture

The dashboard connects to the microservices backend through REST APIs:

1. **Orchestrator Service** (http://localhost:8080) - Main coordination service
2. **Input CSV File Processing Service** (https://localhost:8444) - Processes input CSV files
3. **Payments Processing Service** (https://localhost:8445) - Handles payment processing
4. **Payment Status Service** (https://localhost:8446) - Processes payment statuses
5. **Output CSV File Processing Service** (https://localhost:8447) - Generates output files

## Getting Started

### Prerequisites

- Node.js (v14 or higher)
- npm (v6 or higher)

### Installation

1. Clone the repository
2. Navigate to the ui-dashboard directory:
   ```bash
   cd ui-dashboard
   ```
3. Install dependencies:
   ```bash
   npm install
   ```

### Running the Application

#### Development Mode

To run the dashboard in development mode:

```bash
npm run dev
```

This will start the Vite development server on http://localhost:5173

#### With Mock Server

To run both the dashboard and a mock backend server:

```bash
npm run dev:all
```

This will start:
- Vite development server on http://localhost:5173
- Mock backend server on http://localhost:8080

#### Production Build

To build the application for production:

```bash
npm run build
```

To preview the production build:

```bash
npm run preview
```

## Project Structure

```
ui-dashboard/
├── public/                 # Static assets
├── src/
│   ├── components/         # React components
│   │   ├── Dashboard.jsx   # Main dashboard component
│   │   ├── FileUpload.jsx  # File upload component
│   │   ├── GaugeChart.jsx  # Reusable gauge chart component
│   │   ├── PipelineGauges.jsx  # Pipeline gauges container
│   │   └── PipelineVisualization.jsx  # Pipeline visualization
│   ├── services/           # API and WebSocket services
│   │   ├── apiService.js   # REST API service
│   │   └── webSocketService.js  # WebSocket service
│   ├── App.css             # Global styles
│   ├── App.jsx             # App root component
│   └── main.jsx            # Entry point
├── mock-server/            # Mock backend server
│   └── server.js           # Express server with Socket.io
├── index.html              # HTML template
├── package.json            # Project dependencies and scripts
└── vite.config.js          # Vite configuration
```

## Technologies Used

- **React** - Frontend library
- **Vite** - Build tool and development server
- **Recharts** - Data visualization library
- **React Dropzone** - Drag and drop file upload
- **Axios** - HTTP client
- **Express** - Backend server framework
- **Socket.io** - Real-time communication (WebSocket)

## Development

### Adding New Features

1. Create new components in the `src/components/` directory
2. Add new services in the `src/services/` directory
3. Update the main `App.css` file for styling
4. Test changes in development mode

### Connecting to Real Backend

To connect to the real backend services, ensure all microservices are running on their respective ports:
- Orchestrator Service: http://localhost:8080
- Input CSV File Processing Service: https://localhost:8444
- Payments Processing Service: https://localhost:8445
- Payment Status Service: https://localhost:8446
- Output CSV File Processing Service: https://localhost:8447

### Handling Self-Signed Certificates

During development, the backend services use self-signed SSL certificates. When connecting to these services, your browser may show a security warning. This is expected in a development environment.

To proceed:
1. When you see a security warning, click "Advanced" or "Details"
2. Choose "Proceed to localhost (unsafe)" or similar option
3. You may need to do this for each service port (8444, 8445, 8446, 8447)

Note: These warnings only appear in development. In a production environment, proper SSL certificates should be used.
