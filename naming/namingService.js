var grpc = require('@grpc/grpc-js');
var protoLoader = require('@grpc/proto-loader');
var PROTO_PATH =  require('path').join(__dirname, '../protos/naming.proto');
var packageDefinition = protoLoader.loadSync(PROTO_PATH);
var naming_proto = grpc.loadPackageDefinition(packageDefinition).naming;

const registry=[];

function Register(call, callback){
    try{
        const { name, address } = call.request;
        if(!name || !address){
            return callback({code: grpc.status.INVALID_ARGUMENT, message: 'Name and address are required'});
        }
        registry[name] = address;
        console.log(`[NamingService] Registered: ${name} at ${address}`);
        callback(null, { success: true, message: `${name}  registered successfully at ${address}` });
    }
    catch(error){
        console.error('[NamingService] Error registering service:', error);
        callback({code: grpc.status.INTERNAL, message: 'Internal server error'});
    }
}

function Lookup(call, callback) {
    const { name } = call.request;
    const address = registry[name];
    if (!address) {
        console.log(`[NamingService] Lookup FAILED: ${name} not found`);
        return callback(null, { address: '', found: false });
    }
    console.log(`[NamingService] Lookup successful for: ${name}, address: ${address}`);
    callback(null, { address, found: true });
}
function ListServices(call) {
    const entries = Object.entries(registry);
    console.log(`[Naming Service] Listing ${entries.length} service(s)`);
    entries.forEach(([name, address]) => call.write({ name, address }));
    call.end();
}

var server = new grpc.Server();
server.addService(naming_proto.NamingService.service, { Register, Lookup, ListServices });

server.bindAsync("0.0.0.0:50051", grpc.ServerCredentials.createInsecure(), function(err, port){
    if(err){
        console.error('[NamingService] Server binding ERROR:', err);
        process.exit(1);
        return;
    }
    console.log('[NamingService] Server listening on port:', port);
})