const opc = require('node-opcua')
const prompts = require("prompts")
const { IsDeviceOk } = require('./opc-functions')
const iot = require("azure-iot-device")
const _ = require("lodash")
const { ENV_PREFIX } = require('./constants')
const fs = require('fs/promises')
const isPathValid = require("is-valid-path")
const ConfLib = require("conf").default;

module.exports = async function CreateConfig() {
    const SavedAnswers = new ConfLib();

    const ora = (await import("ora")).default
    const chalk = (await import('chalk')).default

    const resUrl = await prompts([
        {
            type: "text",
            name: "opcua-server-url",
            initial: SavedAnswers.get("opcua-server-url") || "",
            message: "What is your OPC UA server url?",
            validate(input) {
                try {
                    opc.is_valid_endpointUrl(input) // this function is really bad
                    return true
                }
                catch {
                    return "Invalid URL, please provide a valid url";
                }
            }
        }
    ], { onCancel() { process.exit(0) } })

    SavedAnswers.set('opcua-server-url', resUrl['opcua-server-url'])

    const spinner = ora("Connecting to the server").start()

    try {
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
        await opcclient.connect(resUrl['opcua-server-url'])
        var session = await opcclient.createSession()
    } catch (err) {
        spinner.stop()
        console.error(chalk.red("Could not connect to the provided server, error: \n"), err.message)
        return
    }

    spinner.stop()
    spinner.start("Searching through devices")

    const objects = await session.browse("ns=0;i=85")

    spinner.stop()

    const early_candidate_devices = objects.references.filter(device => device.nodeId.namespace !== 0)
    const device_info = await Promise.all(early_candidate_devices.map(async device => ({ device, isOk: await IsDeviceOk(session, device.nodeId.toString()) })))

    const remeberedDevices = SavedAnswers.get('selected-devices') || []
    const resDevices = await prompts({
        name: "selected-devices",
        type: "multiselect",
        message: "Select devices to include in configuration",
        choices: device_info.map(({ device, isOk }) => ({
            title: device.displayName.text,
            disabled: !isOk,
            selected: remeberedDevices.includes(device.nodeId.toString())
        }))
    }, { onCancel() { process.exit(0) } })
    SavedAnswers.set('selected-devices', resDevices['selected-devices'].map(idx => device_info[idx].device.nodeId.toString()))

    const resKeys = await prompts(resDevices['selected-devices'].map(idx => ({
        type: "password",
        name: idx,
        initial: SavedAnswers.get(`iotkey-${idx}`),
        message: `Azure IoT device connection string for ${device_info[idx].device.displayName.text}`,
        validate(input) {
            try {
                iot.ConnectionString.parse(input)
                return true
            } catch (err) {
                return "Invalid connection string"
            }
        },
    })), {
        onSubmit(prompt, answer) {
            SavedAnswers.set(`iotkey-${prompt.name}`, answer)
        },
        onCancel() { process.exit(0) }
    })

    const resIntervals = await prompts([
        {
            name: 'interval-read',
            type: 'number',
            message: 'How often to read from the OPC UA server',
            initial: SavedAnswers.get("interval-read") || 1000,
            validate(input) {
                if (input < 0) {
                    return "Must be positive"
                }
                return true
            }
        },
        {
            name: 'interval-send',
            type: 'number',
            message: 'How often to send data to the Azure cloud',
            initial: SavedAnswers.get("interval-send") || 5000,
            validate(input) {
                if (input < 0) {
                    return "Must be positive"
                }
                return true
            }
        }
    ])
    SavedAnswers.set('interval-read', resIntervals['interval-read'])
    SavedAnswers.set('interval-send', resIntervals['interval-send'])

    const apps = resDevices['selected-devices'].map(idx => ({
        name: `IoT-Agent-${device_info[idx].device.browseName.name}`,
        script: process.argv[1],
        env: {
            [`${ENV_PREFIX}_ENDPOINT`]: resUrl['opcua-server-url'],
            [`${ENV_PREFIX}_DEVICE`]: device_info[idx].device.nodeId.toString(),
            [`${ENV_PREFIX}_CONNECTION_STRING`]: resKeys[idx],
            [`${ENV_PREFIX}_READ_INTERVAL`]: resIntervals['interval-read'],
            [`${ENV_PREFIX}_SEND_INTERVAL`]: resIntervals['interval-send']
        },
        max_restarts: 10
    }))

    const resFilename = await prompts({
        name: "config-name",
        type: 'text',
        initial: SavedAnswers.get("config-name"),
        message: "Path to the configuration file",
        initial: "factoryecosystem.config.js",
        validate(input) {
            if (isPathValid(input)) {
                return true
            } else {
                return "Invalid path"
            }
        }
    }, { onCancel() { process.exit(0) } })
    SavedAnswers.set("config-name", resFilename['config-name'])

    await fs.writeFile(resFilename['config-name'], `module.exports = { apps: ${JSON.stringify(apps, undefined, 2)} }`)

    console.log(chalk.green(`Succesfully generated the ecosystem file ${resFilename['config-name']}`))
}