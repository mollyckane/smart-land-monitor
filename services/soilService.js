var grpc = require('@grpc/grpc-js');
var protoLoader=require('@grpc/proto-loader');
var PROTO_PATH=require('path').join(__dirname, '../protos/soil.proto');

var packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase:true,
    longs:String,
    enums:String,
    defaults:true,
    oneofs:true
});

//load soil.proto
var soil_proto=grpc.loadPackageDefinition(packageDefinition).soil;

const SOIL_TYPES = ['Clay', 'Sandy', 'Loamy', 'Silt', 'Peat'];

//generate soil data
function generateSoilData(zone_id){
    //random moisture value
    const moisture = parseFloat((10 + Math.random() * 70).toFixed(1));
    const soil_ph = parseFloat((4 + Math.random() * 2).toFixed(1));

    //get erosion risk
    let erosion_risk;
    if(moisture < 20){
        erosion_risk = 'HIGH';
    }
    else if(moisture < 40){
        erosion_risk='MEDIUM';
    }
    else{
        erosion_risk='LOW';
    }

    return {
        zone_id,
        moisture_percent: moisture,
        erosion_risk,
        soil_type: SOIL_TYPES[Math.floor(Math.random() * SOIL_TYPES.length)],
        soil_ph: soil_ph,
        timestamp: new Date().toISOString()
    };
}

// RPC 1: Unary RPC - GetSoilStatus
function GetSoilStatus(call, callback){
    //try-catch block
    try{
        const { zone_id} = call.request;

    if(!zone_id){
        return callback({
            code: grpc.status.INVALID_ARGUMENT,
            message: 'Zone ID is required.'
        });
    }

    const status = generateSoilData(zone_id);
    console.log(`[Soil] Current reading for ${zone_id}: Moisture: ${status.moisture_percent}%, Risk: ${status.erosion_risk}, pH: ${status.soil_ph}`);

    callback(null, status);
    }
    catch(error){
        console.error('[Soil] Error in GetSoilStatus:', error);

        callback({
            code: grpc.status.INTERNAL, 
            message: 'Internal server error'
        });   
    }  
}

//RPC 2: Client Streaming RPC - UploadReadingHistory
function UploadReadingHistory(call, callback){
    try{
         console.log('[Soil] Retrieving readings from client...');

        const readings=[];

        //receive readings that client streams to us
        call.on('data', (reading) => {
            try{
                if(!reading.zone_id){
                    console.warn('[Soil] Received reading with missing zone_id');
                    return;
                }
                readings.push(reading);
                console.log(`[Soil] Received reading for Zone:${reading.zone_id}, Moisture: ${reading.moisture_percent}%, pH: ${reading.soil_ph}, Time: ${readings.timestamp}`);
            }
            catch(error){
                console.error('[Soil] Error processing uploaded reading:', error);
            }
        });

        //client finished streaming - analyse and respond
        call.on('end', () =>{
            try{
                if(readings.length === 0){
                    return callback({
                        code: grpc.status.INVALID_ARGUMENT,
                        message: 'No readings were uploaded'
                    });
                }
                const total=readings.length;
                const avgMoisture=parseFloat((readings.reduce((sum, r) => sum + r.moisture_percent, 0)/total).toFixed(2));
                const zone_id=readings[0].zone_id;

                let risk_summary;

                if(avgMoisture<20){
                    risk_summary=`CRITICAL - Average moisture of ${avgMoisture}% indicates severe desertification risk in ${zone_id}`;
                }
                else if(avgMoisture<40){
                    risk_summary=`WARNING - Average moisture of ${avgMoisture}% indicates moderate land degradation risk in ${zone_id}`;
                }
                else{
                    risk_summary=`STABLE - Average moisture of ${avgMoisture}% indicates healthy soil conditions in ${zone_id}`;
                }
                console.log(`[Soil] Analysis complete: ${total} readings, Average moisture: ${avgMoisture}%`);
                console.log(`[Soil] Risk Summary: ${risk_summary}`);

                callback(null, {
                    average_moisture: avgMoisture,
                    risk_summary,
                    total_readings: total,
                    zone_id
                });
            }   
            catch(error){
                console.error('[Soil] Error analysing UploadReadingHistory stream:', error);
                callback({
                    code: grpc.status.INTERNAL,
                    message: 'Internal server error while analysing readings'
                });
            }
        });
        call.on('error', (err) =>{
            console.error('[Soil] UploadReadingHistory error:', err.message);
        });
    }
    catch(error){
        console.error('[Soil] Error processing UploadReadingHistory data: ', error);

        callback({
            code: grpc.status.INTERNAL,
            message: 'Internal server error'
        });
    }
}

// RPC 3: Streaming RPC - StreamZoneMonitor
function StreamZoneMonitor(call){
    let interval;

    try{
        const { zone_id} = call.request;

        //error handling
        if(!zone_id){
            call.destroy({
                code: grpc.status.INVALID_ARGUMENT,
                message: 'Zone ID is required'
            });
            return;
        }
        console.log(`[Soil] Streaming Zone Monitor started for ${zone_id}`);

        let count=0;
        const MAX_READINGS=8;

        interval = setInterval(() =>{
            try{
                //end streaming once it reaches max readings
                if(count>=MAX_READINGS){
                    clearInterval(interval);
                    call.end();
                    console.log(`[Soil] Streaming Zone Monitor completed for ${zone_id}`);
                    return;
                }

                const status = generateSoilData(zone_id);
                const reading={
                    zone_id: status.zone_id,
                    moisture_percent: status.moisture_percent,
                    erosion_risk: status.erosion_risk,
                    soil_type: status.soil_type,
                    soil_ph: status.soil_ph,
                    timestamp: status.timestamp
                };

                call.write(reading);
                console.log(`[Soil] Streamed reading ${++count}/${MAX_READINGS} for ${zone_id}`);
            }
            catch(error){
                console.error('[Soil] Error streaming reading:', error);
                clearInterval(interval);
                call.destroy({
                    code: grpc.status.INTERNAL,
                    message: 'Internal server error while streaming'
                });
            }
        }, 1500);

        //if client disconnects early
        call.on('cancelled',()=>{
            clearInterval(interval);
            console.log(`[Soil] Streaming Live Readings cancelled by client for ${zone_id}`);
        });    
    }
    catch(error){
        console.error('[Soil] Error in StreamZoneMonitor:', error);
        clearInterval(interval);
        call.destroy({
            code: grpc.status.INTERNAL,
            message: 'Internal server error'
        });
    }
}

//Register with naming client
var namingProto = grpc.loadPackageDefinition(protoLoader.loadSync(require('path').join(__dirname, '../protos/naming.proto'))).naming;

function registerWithNamingService(callback){
    try{
        const namingClient = new namingProto.NamingService('localhost:50051', grpc.credentials.createInsecure());

        //deadline to prevent hanging
        const deadline = new Date();
        deadline.setSeconds(deadline.getSeconds() + 5);

        namingClient.Register(
            { name: 'SoilSensor', address: 'localhost:50053' },
            { deadline },
            (err, response) =>{
                if(err){
                    console.error('[Soil] Could not register with Naming Service: ', err);
                    console.warn('[Soil] WARNING: Service will not be discoverable by clients');
                    return;

                }
                console.log('[Soil] Successfully registered with Naming Service:', response.message);
            }
        );
    }
    catch(error){
        console.error('[Soil] Error registering with Naming Service:', error);
    }
} 

//main method
function main(){
    const server= new grpc.Server();

    server.addService(soil_proto.SoilSensor.service, {
        GetSoilStatus,
        UploadReadingHistory,
        StreamZoneMonitor
    });

    server.bindAsync('0.0.0.0:50053', grpc.ServerCredentials.createInsecure(), (err, port)=>{
        if(err){
            console.error('[Soil] Server binding ERROR:', err);
            process.exit(1);
            return;
        }
        console.log('[Soil] Server listening on port: ', port);
        // server.start(); not needed according to terminal

        //register with naming service after server starts
        registerWithNamingService();
    });
   
}

main();
