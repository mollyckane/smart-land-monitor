var grpc = require('@grpc/grpc-js');
var protoLoader = require('@grpc/proto-loader');
var PROTO_PATH =  require('path').join(__dirname, '../protos/naming.proto');
var packageDefinition = protoLoader.loadSync(PROTO_PATH);
var naming_proto = grpc.loadPackageDefinition(packageDefinition).naming;

const registry={
    "ForestService":"localhost:50052",
    "SoilSensor":"localhost:50053",
    "WaterService":"localhost:50054"
};

function Register(call, callback){
    try{
        const { name, address } = call.request;
        if(!name || !address){
            return callback({code: grpc.status.INVALID_ARGUMENT, message: 'Name and address are required'});
        }
        registry[name] = address;
        console.log(`[Naming Service] Registered: ${name} at ${address}`);
        callback(null, { success: true, message: `${name}  registered successfully at ${address}` });
    }
    catch(error){
        console.error('[Naming Service] Error registering service:', error);
        callback({code: grpc.status.INTERNAL, message: 'Internal server error'});
    }
}

function Lookup(call, callback){
    try{
        const { name } = call.request;
        const address = registry[name];
        if(!address){
            console.log(`[Naming Service] Lookup failed for: ${name}, not found.`);
            return callback({code: grpc.status.NOT_FOUND, message: `${name} not found.`});
        }
        console.log(`[Naming Service] Lookup successful for: ${name}, address: ${address}`);
        callback(null, { address });
    }
    catch(error){
        console.error('[Naming Service] Error looking up service:', error);
        callback({code: grpc.status.INTERNAL, message: 'Internal server error'});
    }
}

function ListServices(call, callback){
    try{
        const entries = Object.entries(registry);
        console.log(`[Naming Service] Listing services: ${entries.length} service(s) found.`);
        callback(null, { services: entries });
    }
    catch(error){
        console.error('[Naming Service] Error listing services:', error);
        callback({code: grpc.status.INTERNAL, message: 'Internal server error'});
    }
}

var server = new grpc.Server();
server.addService(naming_proto.NamingService.service, { Register, Lookup, ListServices });

server.bindAsync("0.0.0.0:50051", grpc.ServerCredentials.createInsecure(), function(err, port){
    if(err){
        console.error('[Naming Service] Server binding ERROR:', err);
        process.exit(1);
        return;
    }
    console.log('[Naming Service] Server listening on port:', port);
})