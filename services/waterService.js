var grpc = require('@grpc/grpc-js');
var protoLoader=require('@grpc/proto-loader');
var PROTO_PATH=require('path').join(__dirname, '../protos/water.proto');

var packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase:true,
    longs:String,
    enums:String,
    defaults:true,
    oneofs:true
});

//load water.proto
var water_proto=grpc.loadPackageDefinition(packageDefinition).water;

//generate water data
function generateWaterQuality(source_id) {
    const pollutant_ppm = parseFloat((Math.random() * 15).toFixed(2));
    let status;
    if (pollutant_ppm > 10){
        status = 'CONTAMINATED';
    } 
    else if (pollutant_ppm > 5){
        status = 'CAUTION';
    } 
    else {
        status = 'CLEAN';
    }

    return {
        ph_level: parseFloat((6.5 + Math.random() * 2).toFixed(2)), //random ph value
        pollutant_ppm,
        flow_rate: parseFloat((2 + Math.random() * 10).toFixed(2)), //random flow rate
        status
    };
}

//RPC 1: Unary RPC - WaterQuality
function GetWaterQuality(call, callback){
    try{
        const { source_id } = call.request;

        if (!source_id) {
            return callback({
                code: grpc.status.INVALID_ARGUMENT,
                message: 'Source ID is required'
            });
        }
        const status = generateWaterQuality(source_id);
        console.log(`[Water] Getting water quality for ${source_id}: pH: ${status.ph_level}, Turbidity: ${status.pollutant_ppm}ppm, Flow: ${status.flow_rate}L/s, Status=${status.status} `);
        callback(null, status);
    }
    catch(error){
        console.error(`[Water] Error in GetWaterQuality: `, error);
        callback({
            code: grpc.status.INTERNAL,
            message: 'Internal service error'
        });
    }
}

//RPC 2: Server Streaming RPC - StreamWaterLevels
function StreamWaterLevels(call){
    let interval;
    try{
        const { source_id } = call.request;

        if (!source_id) {
            call.destroy({ 
                code: grpc.status.INVALID_ARGUMENT,
                message: 'Source ID is required' 
            });
            return;
        }

        console.log(`[Water] Streaming Water Levels started for: ${source_id}`);
        let count = 0;
        const MAX_READINGS=8;

        interval = setInterval(() => {
            try{
                if (count >= MAX_READINGS) {
                    clearInterval(interval);
                    call.end();
                    return;
                }

                const reading={
                    source_id,
                    level_metres: parseFloat((6.5 + Math.random() * 2).toFixed(2)),
                    timestamp:  new Date().toISOString()
                };
                call.write(reading);
                console.log(`[Water] Streamed water levels ${++count}/${MAX_READINGS} for ${source_id}`);
            }
            catch(error){
                console.error('[Water] Error streaming reading:', error);
                clearInterval(interval);
                call.destroy({
                    code: grpc.status.INTERNAL,
                    message: 'Internal server error while streaming'
                });
            }
        }, 1500);

        call.on('cancelled', () => {
            clearInterval(interval);
            console.log(`[Water] Stream cancelled by client`);
        });
    }
    catch(error){
        console.error('[Water] Error in StreamWaterLevels: ', error);
        clearInterval(interval);
        call.destroy({
            code: grpc.status.INTERNAL,
            message: 'Internal server error'
        });
    }
}

//RPC 3: Client Streaming - PollutionReport
function ReportPollutionEvent(call, callback) {
    try {
        console.log('[Water] Receiving pollution reports from client...');

        const readings = [];
        let reportCount = 0;
        call.on('data', (report) => {
            try {
                if (!report.source_id) {
                    console.warn('[Water] Received reading with missing source_id');
                    return;
                }
                readings.push(report);
                console.log(`[Water] Received pollution report #${++reportCount}: Source: ${report.source_id}, Pollutant: ${report.pollutant_ppm}`);
            }
            catch (error) {
                console.error('[Water] Error processing uploaded reports: ', error);
            }
        });

        call.on('end', () => {
            try {
                console.log(`[Water] Completed receiving pollution reports. Total reports: ${reportCount}`);

                if (reportCount === 0) {
                    return callback({
                        code: grpc.status.INVALID_ARGUMENT,
                        message: 'No readings received'
                    });
                }
                const max_pollutant = Math.max(...readings.map(r => r.pollutant_ppm));
                const event_id = `EVT-${Date.now()}`;
                console.log(`[Water] Event logged: ${event_id} reported with max pollutant level: ${max_pollutant}ppm`);

                callback(null, {
                    event_id,
                    readings_logged: reportCount,
                    max_pollutant: parseFloat(max_pollutant.toFixed(2))
                });
            }
            catch (error) {
                console.error('[Water] Error analysing ReportPollutionEvent stream:', error);
                callback({
                    code: grpc.status.INTERNAL,
                    message: 'Internal server error while analysing readings'
                });
            }
        });
        call.on('error', (err) => {
            console.error(`[Water] Error receiving pollution reports: ${err.message}`);
        });
    }
    catch (error) {
        console.error('[Water] Error processing ReportPollutionEvent data: ', error);
        callback({
            code: grpc.status.INTERNAL,
            message: 'Internal server error'
        });
    }
}

//RPC 4: Bidirectional RPC - PollutionAlertChannel
function PollutionAlertChannel(call) {
    try{
        console.log('[Water] Pollution Alert Channel activated...');

        call.on('data', (config) => {
            const { source_id, alert_threshold_ppm } = config;
            const status = generateWaterQuality(source_id);

            if (status.pollutant_ppm >alert_threshold_ppm) {
                call.write({
                    event_id: `EVT-${Date.now()}`,
                    readings_logged: 1,
                    max_pollutant: status.pollutant_ppm
                });
            }
        });

        call.on('end', () => {
            console.log('[Water] Pollution alert channel closed'); 
            call.end(); 
        });

        call.on('error', (err) => {
            console.error('[Water] Error:', err.message)
        });
    }
    catch(error){
        console.error('[Water] Error in PollutionAlertChannel:', error);
        call.destroy({
            code: grpc.status.INTERNAL,
            message: 'Internal server error'
        });
    }
}

//register with naming client
var namingProto=grpc.loadPackageDefinition(protoLoader.loadSync(require('path').join(__dirname, '../protos/naming.proto'))).naming;

function registerWithNamingService(callback){
    try{
        const namingClient=new namingProto.NamingService('localhost:50051', grpc.credentials.createInsecure());

        //deadline to prevent hanging
        const deadline=new Date();
        deadline.setSeconds(deadline.getSeconds() + 5);

        namingClient.Register(
            { name: 'WaterMonitor', address: 'localhost:50054'},
            { deadline },
            (err, response)=>{
                if(err){
                    console.error('[Water] Could not register with Naming Service: ', err);
                    console.warn('[Water] WARNING: Service will not be discoverable by clients');
                    return;
                }
                console.log('[Water] Successfully registered with Naming Service');
            }
        );
    }
    catch(error){
        console.error('[Water] Error registering with Naming Service: ', error);
    }
}

//main method
function main(){
    const server= new grpc.Server();

    server.addService(water_proto.WaterMonitor.service, {
        GetWaterQuality,
        StreamWaterLevels,
        ReportPollutionEvent,
        PollutionAlertChannel
    });

    server.bindAsync('0.0.0.0:50054', grpc.ServerCredentials.createInsecure(), (err, port)=>{
        if(err){
            console.error('[Water] Server binding ERROR: ', err);
            process.exit(1);
            return;
        }
        console.log('[Water] Server listening on port: ', port);
        // server.start(); not needed according to terminal

        //register with naming service after server starts
        registerWithNamingService();
    });
}

main();
