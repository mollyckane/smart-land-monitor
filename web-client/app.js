var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');

var indexRouter = require('./routes/index');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));


var grpc = require('@grpc/grpc-js');
var protoLoader = require('@grpc/proto-loader');

//proto file paths
var FOREST_PROTO_PATH = require('path').join(__dirname, '../protos/forest.proto');
var SOIL_PROTO_PATH = require('path').join(__dirname, '../protos/soil.proto');
var WATER_PROTO_PATH = require('path').join(__dirname, '../protos/water.proto');
var NAMING_PROTO_PATH = require('path').join(__dirname, '../protos/naming.proto');

//load package definitions for protos
const protoOptions = { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true };
var forest_proto = grpc.loadPackageDefinition(protoLoader.loadSync(FOREST_PROTO_PATH, protoOptions)).forest;
var soil_proto = grpc.loadPackageDefinition(protoLoader.loadSync(SOIL_PROTO_PATH, protoOptions)).soil;
var water_proto = grpc.loadPackageDefinition(protoLoader.loadSync(WATER_PROTO_PATH, protoOptions)).water;
var naming_proto = grpc.loadPackageDefinition(protoLoader.loadSync(NAMING_PROTO_PATH, protoOptions)).naming;


//create grpc connections with each service
let forestClient = null;
let soilClient = null;
let waterClient = null;
let namingClient = new naming_proto.NamingService('localhost:50051', grpc.credentials.createInsecure());

let forestAlertCall = null;
let waterAlertCall = null;
let forestAlertClients = [];
let waterAlertClients = [];

//helper method for bidirectional streaming
function sendSseEvent(clients, payload) {
    const data = `data: ${JSON.stringify(payload)}\n\n`;

    clients.forEach((clientRes) => {
        clientRes.write(data);
    });
}

//------ discover services --------
function discoverServices() {
    const services = [
        {
            serviceName: 'ForestMonitor',
            serviceProto: forest_proto.ForestMonitor,
            assignClient: (client) => { forestClient = client; }
        },
        {
            serviceName: 'SoilSensor',
            serviceProto: soil_proto.SoilSensor,
            assignClient: (client) => { soilClient = client; }
        },
        {
            serviceName: 'WaterMonitor',
            serviceProto: water_proto.WaterMonitor,
            assignClient: (client) => { waterClient = client; }
        }
    ];

    services.forEach((service) => {
        const serviceName = service.serviceName;
        const serviceProto = service.serviceProto;
        const assignClient = service.assignClient;


        namingClient.Lookup({ name: serviceName }, (err, res) => {
            if (err) {
                console.log(`[Web Client] Lookup error for ${serviceName}: ${err.message}`);
                return;
            }
            if (!res.found) {
                console.log(`[Web Client] Could not find ${serviceName}...check if it is running`);
                return;
            }
            try {
                const discoveredClient = new serviceProto(
                    res.address,
                    grpc.credentials.createInsecure()
                );
                assignClient(discoveredClient);
                console.log(`[Web Client] Discovered ${serviceName} at ${res.address}`);
            }
            catch (error) {
                console.log(`[Web Client] Failed to create client for ${serviceName}: ${error.message}`);
            }
        });
    });
}

//-------- main dashboard page --------
app.get('/', (req, res) => {
    res.render('index');
});

//----------- forest --------------
//rpc 1: get current reading (unary)
app.post('/forest/reading', (req, res) => {
    if (!forestClient) return res.status(503).json({ error: 'Forest Monitor not available' });
    const location = req.body.location || 'Amazon Rainforest';

    //add metadata
    const metadata = new grpc.Metadata();
    metadata.add('client', 'gui-dashboard');
    metadata.add('request-type', 'simple-rpc');

    //set a deadline
    const deadline = new Date();
    deadline.setSeconds(deadline.getSeconds() + 5);

    forestClient.GetCurrentReading({ location }, metadata, { deadline }, (err, response) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(response);
    });
});

//rpc 2: stream forest monitoring (server streaming)
app.get('/forest/stream', (req, res) => {
    if (!forestClient) return res.status(503).json({ error: 'Forest Monitor not available' });
    const location = req.query.location || 'ZoneA';

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const call = forestClient.StreamLiveReadings({ location });

    call.on('data', (reading) => {
        res.write(`data: ${JSON.stringify(reading)}\n\n`);
    });

    call.on('end', () => {
        res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
        res.end();
    });

    call.on('error', (err) => {
        res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
        res.end();
    });

    req.on('close', () => call.cancel());
});

//rpc 3: monitor alert channel (bidirectional)
app.get('/forest/alerts', (req, res) => {
    if (!forestClient) {
        return res.status(503).json({ error: 'Forest Monitor not available' });
    }

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    forestAlertCall = forestClient.MonitorAlertChannel();

    forestAlertCall.on('data', function (alert) {
        res.write(`data: ${JSON.stringify(alert)}\n\n`);
    });

    forestAlertCall.on('end', function () {
        res.end();
    });

    forestAlertCall.on('error', function (err) {
        console.log(err);
        res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
        res.end();
    });

    req.on('close', function () {
        if (forestAlertCall) {
            forestAlertCall.end();
            forestAlertCall = null;
        }
    });
});

app.post('/forest/alerts', (req, res) => {
    if (!forestAlertCall) {
        return res.status(400).json({ error: 'Forest alert stream not started' });
    }

    const location = req.body.location || 'Amazon Rainforest';
    const alert_type = req.body.alert_type || 'FIRE';
    const threshold = parseFloat(req.body.threshold) || 75;

    forestAlertCall.write({
        location,
        alert_type,
        threshold
    });

    res.json({ message: 'Forest alert config sent' });
});

//-------- soil ---------
//rpc 1: get soil status (unary)
app.post('/soil/status', (req, res) => {
    if (!soilClient) return res.status(503).json({ error: 'Soil Sensor not available' });
    const zone_id = req.body.zone_id || 'ZoneB';

    const deadline = new Date();
    deadline.setSeconds(deadline.getSeconds() + 5);

    soilClient.GetSoilStatus({ zone_id }, { deadline }, (err, response) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(response);
    });
});

//rpc 2: stream soil sensor (streaming)
app.get('/soil/stream', (req, res) => {
    if (!soilClient) return res.status(503).json({ error: 'Soil Sensor not available' });
    const zone_id = req.query.zone_id || 'ZoneB';

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const call = soilClient.StreamZoneMonitor({ zone_id });

    call.on('data', (status) => {
        res.write(`data: ${JSON.stringify(status)}\n\n`);
    });

    call.on('end', () => {
        res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
        res.end();
    });

    call.on('error', (err) => {
        res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
        res.end();
    });
    req.on('close', () => call.cancel());
});

//rpc 3: upload 5 simulated historical readings (client streaming)
app.post('/soil/upload', (req, res) => {
    if (!soilClient) return res.status(503).json({ error: 'Soil Sensor not available' });
    const zone_id = req.body.zone_id || 'ZoneB';

    const call = soilClient.UploadReadingHistory((err, result) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(result);
    });
    //simulate streaming 5 historical readings to the service
    const readings = [
        { zone_id, moisture_percent: 18.5, timestamp: '2026-04-10T08:00:00Z' },
        { zone_id, moisture_percent: 22.1, timestamp: '2026-04-10T12:00:00Z' },
        { zone_id, moisture_percent: 15.3, timestamp: '2026-04-11T08:00:00Z' },
        { zone_id, moisture_percent: 19.8, timestamp: '2026-04-11T12:00:00Z' },
        { zone_id, moisture_percent: 12.4, timestamp: '2026-04-12T08:00:00Z' }
    ];
    readings.forEach(r => call.write(r));
    call.end();
});

//-------- water --------
//rpc 1: get water quality (unary)
app.post('/water/quality', (req, res) => {
    if (!waterClient) return res.status(503).json({ error: 'Water Tracker not available' });
    const source_id = req.body.source_id || 'RiverAlpha';

    const metadata = new grpc.Metadata();
    metadata.add('client', 'gui-dashboard');

    const deadline = new Date();
    deadline.setSeconds(deadline.getSeconds() + 5);

    waterClient.GetWaterQuality({ source_id }, { metadata, deadline }, (err, response) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(response);
    });
});

//rpc 3: stream water levels
app.get('/water/stream', (req, res) => {
    if (!waterClient) return res.status(503).json({ error: 'Water Tracker not available' });
    const source_id = req.query.source_id || 'RiverAlpha';

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const call = waterClient.StreamWaterLevels({ source_id });

    call.on('data', (level) => {
        res.write(`data: ${JSON.stringify(level)}\n\n`);
    });

    call.on('end', () => {
        res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
        res.end();
    });

    call.on('error', (err) => {
        res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
        res.end();
    });
    req.on('close', () => call.cancel());
});

//rpc 2: client streaming — report a pollution event with multiple readings
app.post('/water/pollution', (req, res) => {
    if (!waterClient) return res.status(503).json({ error: 'Water Tracker not available' });

    const source_id = req.body.source_id || 'RiverAlpha';
    const pollutant = parseFloat(req.body.pollutant_ppm) || 8.5;
    const count = parseInt(req.body.count) || 3;

    const call = waterClient.ReportPollutionEvent((err, result) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(result);
    });

    //build readings using the user's inputted pollutant level
    //add slight variation to each reading to make it realistic
    for (let i = 0; i < count; i++) {
        call.write({
            source_id,
            pollutant_ppm: parseFloat((pollutant + (Math.random() * 2 - 1)).toFixed(2)),
            timestamp: new Date().toISOString()
        });
    }
    call.end();
});

//rpc 4: pollution alert channel (bidirectional)
app.get('/water/alerts', (req, res) => {
    if (!waterClient) {
        return res.status(503).json({ error: 'Water Tracker not available' });
    }

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    waterAlertCall = waterClient.PollutionAlertChannel();

    waterAlertCall.on('data', function (alert) {
        res.write(`data: ${JSON.stringify(alert)}\n\n`);
    });

    waterAlertCall.on('end', function () {
        res.end();
    });

    waterAlertCall.on('error', function (err) {
        console.log(err);
        res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
        res.end();
    });

    req.on('close', function () {
        if (waterAlertCall) {
            waterAlertCall.end();
            waterAlertCall = null;
        }
    });
});

app.post('/water/alerts', (req, res) => {
    if (!waterAlertCall) {
        return res.status(400).json({ error: 'Water alert stream not started' });
    }

    const source_id = req.body.source_id || 'RiverAlpha';
    const threshold_ppm = parseFloat(req.body.threshold_ppm) || 8.5;

    waterAlertCall.write({
        source_id,
        threshold_ppm
    });

    res.json({ message: 'Water monitor request sent' });
});

// ----- naming service -------
app.get('/services/list', (req, res) => {
    const services = [];
    const call = namingClient.ListServices({});

    call.on('data', (service) => services.push(service));
    call.on('end', () => res.json(services));
    call.on('error', (err) => res.status(500).json({ error: err.message }));
});



app.use('/', indexRouter);

// catch 404 and forward to error handler
app.use(function (req, res, next) {
    next(createError(404));
});

// error handler
app.use(function (err, req, res, next) {
    // set locals, only providing error in development
    res.locals.message = err.message;
    res.locals.error = req.app.get('env') === 'development' ? err : {};

    // render the error page
    res.status(err.status || 500);
    res.render('error');
});

discoverServices();

module.exports = app;