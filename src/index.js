const yargs = require('yargs/yargs')
const { hideBin } = require('yargs/helpers')
const opc = require('node-opcua')

const args = yargs(hideBin(process.argv))
    .config()
    .env("IOTCONFIG")
    .option('verbose', { alias: 'v', type: 'boolean', description: 'Enable verbose logging' })
    .option('endpoint', { alias: 'e', type: 'string', description: 'The URL of the OPC UA endpoint' })
    .option('device', { alias: 'd', type: 'string', description: 'Name of the device' })
    .option('connection_string', { alias: 'c', type: 'string', description: 'Azure IoT device connection string' })
    .demandOption("endpoint", 'You have to provide the OPC UA endpoint')
    .demandOption('connection_string', "You have to provide the Azure IoT connection string")
    .demandOption('device', "You have to provide the ID of the device")
    .check((argv, aliases) => { if (argv.endpoint.length === 0) throw new Error("Endpoint URL cannot be empty"); return true; })
    .check((argv, aliases) => { if (argv.connection_string.length === 0) throw new Error("Connection string cannot be empty"); return true; })
    .check((argv, aliases) => { if (argv.device.length === 0) throw new Error("Device name cannot be empty"); return true; })
    .check((argv, al) => { if (!opc.is_valid_endpointUrl(argv.endpoint)) throw new Error("Invalid OPCUA endpoint"); return true; })
    .parse()


const INFO = args.verbose ? msg => console.log(msg) : _ => { }
const WARN = msg => console.log(msg)
const ERR = msg => console.error(msg)


/**
 * Runs the contained async functions in reverse order when shutting down
 * @type {Function[]}
 */
const cleanup_stack = []

async function Main() {
    INFO("Setting up signal handling")
    process.addListener('SIGINT', async (signal) => {
        INFO("SIGINT received, cleaning up")
        for (const fn of cleanup_stack.reverse()) {
            await fn();
        }
        process.exitCode = 1
        process.exit()
    })

    INFO("Connecting to OPCUA server")
    const session = await MakeOPCConnection(args.endpoint)

    // setup streams and getseters and methods
    INFO("Creating device interfaces")
    
    // connect to azure
    INFO("Connecting to Azure IoT")

    // start sending messages there
    INFO("Starting operation")
}

/**
 * Connect to the opcua server
 * @param {string} endpoint 
 * @param {Function[]} cleanup_stack 
 */
async function MakeOPCConnection(endpoint) {
    const opcclient = opc.OPCUAClient.create({
        applicationName: "iot-azure-agent",
        connectionStrategy: {
            initialDelay: 1000,
            maxRetry: 3
        },
        securityMode: opc.MessageSecurityMode.None,
        securityPolicy: opc.SecurityPolicy.None,
        endpointMustExist: true
    })

    await opcclient.connect(endpoint)
    INFO("Connected to OPC UA server")
    cleanup_stack.push(async () => await opcclient.disconnect())

    const session = await opcclient.createSession()
    INFO("Created an OPC UA session")
    cleanup_stack.push(async () => await session.close(true))

    return session;
}

Main();