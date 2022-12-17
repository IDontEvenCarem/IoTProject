const yargs = require('yargs/yargs')
const { hideBin } = require('yargs/helpers')
const opc = require('node-opcua')
const xs = require('xstream').default
const _ = require('lodash')
const { MemoryStream } = require('xstream')
const { changes_only } = require('./operators')
const { exit } = require('yargs')
const { ENV_PREFIX } = require('./constants')

let verbose = false

const INFO = msg => verbose ? console.log(msg) : { }
const WARN = msg => console.log(msg)
const ERR = msg => console.error(msg)

import("sudo-block").then(sudoBlock => {
    sudoBlock.default();
     
    yargs(hideBin(process.argv))
    .config()
    .env(ENV_PREFIX)
    .command("config", "Interactively create a PM2 ecosystem file", {}, async (args) => {
        await require("./config-creator")();
        exit(0)
    })
    .command(["*", "run"], "default command, runs the app", (yargs) => {
        yargs
        .option('verbose', { alias: 'v', type: 'boolean', description: 'Enable verbose logging', default: false })
        .option('endpoint', { alias: 'e', type: 'string', description: 'The URL of the OPC UA endpoint' })
        .option('device', { alias: 'd', type: 'string', description: 'Name of the device' })
        .option('connection-string', { alias: 'c', type: 'string', description: 'Azure IoT device connection string' })
        .demandOption("endpoint", 'You have to provide the OPC UA endpoint')
        .demandOption('connection-string', "You have to provide the Azure IoT connection string")
        .demandOption('device', "You have to provide the ID of the device")
        .check((argv, aliases) => { if (argv.endpoint.length === 0) throw new Error("Endpoint URL cannot be empty"); return true; })
        .check((argv, aliases) => { if (argv['connection-string'].length === 0) throw new Error("Connection string cannot be empty"); return true; })
        .check((argv, aliases) => { if (argv.device.length === 0) throw new Error("Device name cannot be empty"); return true; })
        .check((argv, al) => { if (!opc.is_valid_endpointUrl(argv.endpoint)) throw new Error("Invalid OPCUA endpoint"); return true; })
    }, async (args) => {
        verbose = args.verbose
        await Main(args);
    })
    .parse()
}) 
    
    
    
    /**
     * Runs the contained async functions in reverse order when shutting down
     * @type {Function[]}
    */
const cleanup_stack = []

async function Main(args) {
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
    const interfaces = await CreateInterfaces(session, args.device)

    // connect to azure
    INFO("Connecting to Azure IoT")

    // start sending messages there
    INFO("Starting operation")

    // console.log(await interfaces.methods.setProductionRate(50));
    // console.log(await interfaces.methods.emergencyStop());
    // console.log(await interfaces.methods.resetErrorStatus());

    const temp_stream = changes_only(interfaces.telemetryStream.map(read => read.Temperature))
    temp_stream.subscribe({
        next(temp) {
            console.log("temperature:", temp)
        }
    })
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

/**
 * @typedef {Object} DeviceReads
 * @property {string} Name
 * @property {0|1} ProductionStatus
 * @property {string} WorkorderId
 * @property {number} ProductionRate
 * @property {[number, number]} GoodCount 
 * @property {[number, number]} BadCount 
 * @property {number} Temperature
 * @property {0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15} DeviceError 
 */

/**
 * @param {opc.ClientSession} session 
 * @param {string} device_id 
 */
async function CreateInterfaces(session, device_id) {
    if (device_id.endsWith("/")) {
        device_id = device_id.substring(0, device_id.length - 1)
    }

    const fields = [
        'ProductionStatus',
        'WorkorderId',
        'ProductionRate',
        'GoodCount',
        'BadCount',
        'Temperature',
        'DeviceError',
        'EmergencyStop',
        'ResetErrorStatus'
    ]
    const node_ids = fields.map(suf => device_id+'/'+suf)
    
    // validate everything that we need is on the device
    const test_reads = await session.read(node_ids.map(
        id => ({nodeId: id, attributeId: opc.AttributeIds.DisplayName})
    ))
    const all_ok = test_reads.every(val => val.statusCode.value === 0)
    if (!all_ok) {
        const err = new Error(`Device ${device_id} does not provide all the nodes needed for proper operation of this program. `)
        err.message += `Missing fields: ${test_reads.map((v, i) => [v, i]).filter(val => val[0].statusCode.value !== 0).map(val => fields[val[1]])}`
        throw err
    }
    
    // generate the initial object parts
    const current_values = await session.read(
        _(node_ids)
            .dropRight(2) // drop methods
            .map(
                id => ({nodeId: id, attributeId: opc.AttributeIds.Value})
            )
            .value()
    )

    const state = _(fields)
        .zip(current_values)
        .dropRight(2)
        .map(([field, read]) => [field, read.value.value])
        .value()
    
    state.push(["Name", /^[^\;]+;(.+)$/.exec(device_id)[1]]) // add the device name to the object

    // store stuff that could be local to the telemetry stream, but because they dont close async, we need them to be closed async
    const varbox = {
        opc_sub: undefined,
        monitored: undefined,
        monitorItems: undefined,
        changeCallback: undefined
    }

    // create stream
    /** @type {MemoryStream<DeviceReads>} */
    const telemetryStream = xs.createWithMemory({
        async start (listener) {
            varbox.opc_sub = await session.createSubscription2({
                requestedPublishingInterval: 1000,
                requestedLifetimeCount: 100,
                requestedMaxKeepAliveCount: 10,
                maxNotificationsPerPublish: 100,
                publishingEnabled: true,
                priority: 0
            })
            varbox.monitored = await varbox.opc_sub.monitorItems(
                _(node_ids).map(id => ({nodeId: id, attributeId: opc.AttributeIds.Value})).dropRight(2).value(),
                {
                    samplingInterval: 1000,
                    discardOldest: true,
                    queueSize: 10
                },
                opc.TimestampsToReturn.Both
            )
            varbox.changeCallback = (item, data, idx) => {
                state[idx][1] = data.value.value
                listener.next(Object.fromEntries(state))
            }
            varbox.monitored.on('changed', varbox.changeCallback)
        },

        async stop () {
            varbox.monitored.off('changed', varbox.changeCallback)
            await monitored.terminate()
            await opc_sub.terminate()
        }
    })

    cleanup_stack.push(async () => {
        await varbox.monitored.terminate()
        await varbox.opc_sub.terminate()
    })

    const methods = {
        async setProductionRate (new_rate) {
            console.log(device_id+"/ProductionRate");
            return session.write({
                nodeId: device_id+"/ProductionRate",
                value: {
                    value: {
                        dataType: opc.DataType.Int32,
                        value: new_rate
                    }
                },
                attributeId: opc.AttributeIds.Value
            })
        },
        async emergencyStop () {
            const objid = opc.coerceNodeId(device_id)
            const metid = opc.coerceNodeId(device_id+'/EmergencyStop')

            console.log(objid)
            console.log(metid)

            return session.call({
                objectId: objid,
                methodId: metid,
            })
        },
        async resetErrorStatus () {
            return session.call({
                objectId: device_id,
                methodId: device_id+'/ResetErrorStatus'
            })
        }
    }

    return {telemetryStream, methods}
}