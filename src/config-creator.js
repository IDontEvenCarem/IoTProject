const prompts = require("prompts")

module.exports = async function CreateConfig () {
    const res = await prompts([
        {
            "type": "text",
            "name": "opcua-server-url",
            message: "What is your OPC UA server url?"
        }
    ])

    console.log(res)
}