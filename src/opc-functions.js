const opc = require('node-opcua')
const { REQUIRED_FIELDS } = require('./constants')



/**
 * Check, if the device with a given id has all the fields we would expect it to have
 * 
 * @param {opc.ClientSession} session 
 * @param {string} device_id 
 * @returns {bool}
 */
async function IsDeviceOk(session, device_id) {
    const node_ids = REQUIRED_FIELDS.map(suf => device_id+'/'+suf)
    const test_reads = await session.read(node_ids.map(
        id => ({nodeId: id, attributeId: opc.AttributeIds.DisplayName})
    ))
    const all_ok = test_reads.every(val => val.statusCode.value === 0)
    return all_ok
}

module.exports = {
    IsDeviceOk
}