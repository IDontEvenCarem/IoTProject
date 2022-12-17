const REQUIRED_FIELDS = [
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

const ENV_PREFIX = "IOTCONFIG"

module.exports = {
    REQUIRED_FIELDS,
    ENV_PREFIX
}