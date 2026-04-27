var grpc = require('@grpc/grpc-js');
var protoLoader = require('@grpc/proto-loader');
// const { time, clear } = require('console');
var PROTO_PATH = require('path').join(__dirname, '../protos/forest.proto');

var packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,   
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});


var forest_proto = grpc.loadPackageDefinition(packageDefinition).forest;

// sample forest data 
const forestData={
    'amazon': { humidity: 85.5, co2_level: 400.2, oxygen_level: 20.9 },
    'congo': { humidity: 90.1, co2_level: 410.5, oxygen_level: 21.0 },
    'taiga': { humidity: 65.0, co2_level: 420.1, oxygen_level: 20.8 }
};

// helper method to generate forest data
function getForestData(location){
    const key=location.toLowerCase();

    //use the real data if we have it
    if(forestData[key]){
        return forestData[key];
    }
    
    //otherwise generate random data
    return {
        humidity: parseFloat((60 + Math.random() * 35).toFixed(2)),
        co2_level: parseFloat((380 + Math.random() * 60).toFixed(2)),
        oxygen_level: parseFloat((19 + Math.random() * 2).toFixed(2))
    };
}

// 1. Unary RPC: GetCurrentReading
// client sends one request, server sends one response back
function GetCurrentReading(call, callback){
    try{
        const { location } = call.request;

        //guard clause for error handling
        if(!location){
            return callback({
                code: grpc.status.INVALID_ARGUMENT, 
                message: 'Location is required'
            });
        }

        const data = getForestData(location);

        const reading={
            location: location.toLowerCase(),
            humidity: data.humidity,
            co2_level: data.co2_level,
            oxygen_level: data.oxygen_level,
            timestamp: new Date().toISOString()
        };

        //log in console
        console.log(`[Forest] Current reading for ${location}: Humidity: ${reading.humidity}%, CO2: ${reading.co2_level}ppm, O2: ${reading.oxygen_level}% at ${reading.timestamp}`);

        callback(null, reading);
    }
    catch(error){
        console.error('[Forest] Error in GetCurrentReading:', error);
        callback({
            code: grpc.status.INTERNAL, 
            message: 'Internal server error'
        });   
    }
}

//2. Server Streaming RPC: StreamReadings
function StreamLiveReadings(call){
    let interval;
    try{
        const { location } = call.request;
        
        //guard clause for error handling
        if(!location){
            call.destroy({
                code: grpc.status.INVALID_ARGUMENT,
                message: 'Location is required'
            });
            return;
        }
        const normalisedLocation=location.toLowerCase();
        console.log(`[Forest Service] Streaming Live Readings started for ${normalisedLocation}`); 

        //send a reading every 5 seconds
        let count=0;
        const MAX_READINGS=8;

        interval = setInterval(()=>{
            try{
                //end streaming once it reaches max readings7
                if (count >= MAX_READINGS) {
                    clearInterval(interval);
                    call.end();
                    console.log(`[Forest] Streaming Live Readings completed for ${normalisedLocation}`);
                    return;
                }

                const data = getForestData(normalisedLocation);
                const reading={
                    location: normalisedLocation,
                    humidity: data.humidity,
                    co2_level: data.co2_level,
                    oxygen_level: data.oxygen_level,
                    timestamp: new Date().toISOString()
                };

                call.write(reading);

                console.log(`[Forest] Streamed reading ${++count}/${MAX_READINGS} for ${normalisedLocation}`);              
            }
            catch(error){
                console.error('[Forest] Error streaming reading:', error);
                clearInterval(interval);
                call.destroy({
                    code: grpc.status.INTERNAL,
                    message: 'Internal server error while streaming'
                });
            }
        },1500);

        //client disconnects early
        call.on('cancelled',()=>{
            clearInterval(interval);
            console.log(`[Forest] Streaming Live Readings cancelled by client for ${normalisedLocation}`);
        });
    }
    catch(error){
        clearInterval(interval);
        console.error('[Forest] Error in StreamLiveReadings:', error);
        call.destroy({code: grpc.status.INTERNAL, message: 'Internal server error'});
    }
}

//3. Bidirectional Streaming RPC: MonitorAlertChannel
function MonitorAlertChannel(call){
    try{
        console.log('[Forest] MonitorAlertChannel bidirectional stream started');

        call.on('data', (request)=>{
            try{
                const { location, humidity_threshold, co2_threshold} = request;
            
                //guard clause
                if(!location){
                    call.write({
                        location: 'unknown',
                        message: 'Location is required in request',
                        severity: 'WARNING',
                        timestamp: new Date().toISOString()
                    });
                    return;
                }

            const normalisedLocation=location.toLowerCase();
            const data = getForestData(normalisedLocation);

            console.log(`[Forest] MonitorForest checking thresholds for ${normalisedLocation}`);

             //check the humidity levels
            if(data.humidity < humidity_threshold){
                call.write({
                    location: normalisedLocation,
                    message: `Humidity ${data.humidity.toFixed(2)}% is below threshold ${humidity_threshold.toFixed(2)}%`,
                    severity: (humidity_threshold - data.humidity) > 20 ? 'HIGH' : 'MEDIUM',
                    timestamp: new Date().toISOString()
                });
             }
             
            //check the CO2 levels
            if(data.co2_level > co2_threshold){
                call.write({
                    location: normalisedLocation,
                    message: `CO2 level ${data.co2_level.toFixed(2)}ppm is above threshold ${co2_threshold.toFixed(2)}ppm`,
                    severity: (data.co2_level - co2_threshold) > 50 ? 'CRITICAL' : 'WARNING',
                    timestamp: new Date().toISOString()
                });
             }
            }
            catch(error){
                console.error('[Forest] Error processing MonitorAlertChannel data:', error);
                call.write({
                    location: 'unknown',
                    message: 'Error processing request',
                    severity: 'ERROR',
                    timestamp: new Date().toISOString()
                });
            }
        });

        call.on('end',()=>{
            console.log('[Forest] MonitorAlertChannel bidirectional stream ended by client');
            call.end();
        });

        call.on('error',(error)=>{
            console.error('[Forest] MonitorAlertChannel stream error:', error);
        });
    }
    catch(error){
        console.error('[Forest] Error in MonitorAlertChannel:', error);
    }
}

//register with naming service
var namingProto = grpc.loadPackageDefinition(protoLoader.loadSync(require('path').join(__dirname, '../protos/naming.proto'))).naming;

function registerWithNamingService(callback){
    try{
        const namingClient = new namingProto.NamingService('localhost:50051', grpc.credentials.createInsecure());

        //deadline to prevent hanging
        const deadline = new Date();
        deadline.setSeconds(deadline.getSeconds() + 5);

        namingClient.Register(
            { name: 'ForestMonitor', address: 'localhost:50052' },
            { deadline },
            (err, response) =>{
                if(err){
                    console.error('[Forest] Could not register with Naming Service: ', err);
                    console.warn('[Forest] WARNING: Service will not be discoverable by clients');
                    return;

                }
                console.log('[Forest] Successfully registered with Naming Service:', response.message);
            }
        );
    }
    catch(error){
        console.error('[Forest] Error registering with Naming Service: ', error);
    }
} 

//main method
function main(){
    const server=new grpc.Server();

    server.addService(forest_proto.ForestMonitor.service, {
        GetCurrentReading,
        StreamLiveReadings,
        MonitorAlertChannel
    });

    server.bindAsync('0.0.0.0:50052', grpc.ServerCredentials.createInsecure(), (err, port)=>{
        if(err){
            console.error('[Forest] Server binding ERROR:', err);
            process.exit(1);
            return;
        }
        console.log('[Forest] Server listening on port: ', port);
        // server.start(); not needed according to terminal

        //register with naming service after server starts
        registerWithNamingService();
    });

}

main();