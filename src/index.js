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

/**
 * Runs the contained async functions in reverse order when shutting down
 * @type {Function[]}
*/
const cleanup_stack = []

async function do_cleanup() {
    for (const fn of cleanup_stack.reverse()) {
        await fn();
    }
}

async function do_emergency() {
    await do_cleanup()
    process.exitCode = 1
    process.exit()
}

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
        .option('read-interval', { alias: 'r', type: 'number', description: 'How often should the OPC UA server send data to the agent (in milliseconds)', default: 1000})
        .option('send-interval', { alias: 's', type: 'number', description: 'How often should the agent send data to Azure (in milliseconds)', default: 5000})
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
    
    
    


async function Main(args) {
    INFO("Setting up signal handling")
    process.on('beforeExit', do_cleanup)
    process.addListener('SIGINT', async (signal) => {
        INFO("SIGINT received, cleaning up")
        await do_emergency();
    })

    INFO("Connecting to OPCUA server")
    const session = await MakeOPCConnection(args.endpoint)

    // setup streams and getseters and methods
    INFO("Creating device interfaces")
    const interfaces = await CreateInterfaces(session, args.device, parseInt(args['read-interval']))

    // connect to azure
    INFO("Connecting to Azure IoT")
    const azureInterfaces = await CreateAzureInterfaces(args['connection-string'], interfaces, parseInt(args['send-interval']))

    // start sending messages there
    // INFO("Starting operation")

    // console.log(await interfaces.methods.setProductionRate(50));
    // console.log(await interfaces.methods.emergencyStop());
    // console.log(await interfaces.methods.resetErrorStatus());
    INFO("Ready!")
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
 * @param {number} readInterval
 */
async function CreateInterfaces(session, device_id, readInterval) {
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
                requestedPublishingInterval: readInterval,
                requestedLifetimeCount: 100,
                requestedMaxKeepAliveCount: 10,
                maxNotificationsPerPublish: 100,
                publishingEnabled: true,
                priority: 0
            })
            varbox.monitored = await varbox.opc_sub.monitorItems(
                _(node_ids).map(id => ({nodeId: id, attributeId: opc.AttributeIds.Value})).dropRight(2).value(),
                {
                    samplingInterval: readInterval,
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

/**
 * 
 * @param {string} connectionString 
 * @param {Awaited<ReturnType<CreateInterfaces>>} interfaces
 * @param {number} sendInterval
 */
async function CreateAzureInterfaces(connectionString, interfaces, sendInterval) {
    const Protocol = require('azure-iot-device-mqtt').Mqtt
    const Client = require('azure-iot-device').Client
    const Message = require('azure-iot-device').Message

    const client = Client.fromConnectionString(connectionString, Protocol)
    client.on('message', ( /** @type {Message} */ msg) => {
        console.log("Got message: ", msg.data.toString()) // temporary
    })
    
    const connected = await client.open()
    cleanup_stack.push(() => client.close())

    // Copy telemetry into our own scope, as we might provide changes at differing rates (could use an operator)
    /** @type {DeviceReads} */
    let televalue = undefined
    interfaces.telemetryStream.subscribe({
        next(reads) {
            televalue = reads
        },
        complete() {
            ERR("Error - telemetry stream closed unexpectedly.")
            do_emergency()
        },
        error(err) {
            ERR("Error - telemetry stream experienced an error. Message: " + err)
            do_emergency()
        }
    })

    // device twin setup
    const twin = await client.getTwin()
        twin.on('properties.desired.ProductionRate', async (delta) => {
        await interfaces.methods.setProductionRate(delta)
    })

    // create a stream of error changes
    interfaces.telemetryStream.map(read => read.DeviceError).compose(changes_only).subscribe({
        next(error) {
            // send D2C message
            const data = {
                type: 'error',
                DeviceError: error
            }
            const msg = new Message(JSON.stringify(data))
            client.sendEvent(msg)

            // update twin
            twin.properties.reported.update({Error: error, LastErrorDate: Date.now()}, (err) => { if (err) ERR(err)})
        }
    })

   

    interfaces.telemetryStream.map(read => read.ProductionRate).compose(changes_only).remember().subscribe({
        next(ProductionRate) {
            twin.properties.reported.update({ProductionRate}, (err) => { if (err) ERR(err)})
        } 
    })

    // direct methods
    client.onDeviceMethod("EmergencyStop", async (req, res) => {
        const opcres = await interfaces.methods.emergencyStop()
        if (opcres.statusCode.value === 0) {
            res.send(200)
        } else {
            res.send(400, opcres.statusCode.toString())
        }
    })

    client.onDeviceMethod("ResetErrorStatus", async (req, res) => {
        const opcres = await interfaces.methods.resetErrorStatus()
        if (opcres.statusCode.value === 0) {
            res.send(200)
        } else {
            res.send(400, opcres.statusCode.toString())
        }
    })

    client.onDeviceMethod("MaintenanceDone", async (req, res) => {
        twin.properties.reported.update({LastMaintenanceDate: Date.now()}, (err) => { if (err) ERR(err)})
    })

    // send telemetry to the cloud
    function SendTelemetry () {
        if (televalue !== undefined) {
            const data = {
                ... _.omit(televalue, 'DeviceError', 'ProductionRate'),
                GoodCount: televalue.GoodCount[1],
                BadCount: televalue.BadCount[1],
                type: 'telemetry'
            }
            const msg = new Message(JSON.stringify(data))
            client.sendEvent(msg)
        } else {
            WARN("OPCUATelemetry was not ready for Azure Sending")
        }

        setTimeout(SendTelemetry, sendInterval)
    }
    setTimeout(SendTelemetry, sendInterval)
}