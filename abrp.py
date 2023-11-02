from time import monotonic
from json import dumps
import logging
from aiohttp import ClientSession, ClientConnectionError, ContentTypeError

log = logging.getLogger('ABRP')

PID_MAP = {
    'SOC_DISPLAY':      ['soc', 1],                 # %
    'dcBatteryPower':   ['power', 2],               # kW
    'speed':            ['speed', 1],               # km/h
    'latitude':         ['lat', 9],                 # °
    'longitude':        ['lon', 9],                 # °
    'charging':         ['is_charging', 0],         # bool 1/0
    'rapidChargePort':  ['is_dcfc', 0],             # bool 1/0
    'isParked':         ['is_parked', 0],           # bool 1/0
    'cumulativeEnergyCharged': ['kwh_charged', 2],  # kWh
    'soh':              ['soh', 1],                 # %
    'heading':          ['heading', 2],             # °
    'altitude':         ['elevation', 1],           # m
    'externalTemperature':   ['ext_temp', 1],       # °C
    'batteryAvgTemperature': ['batt_temp', 1],      # °C
    'dcBatteryVoltage': ['voltage', 2],             # V
    'dcBatteryCurrent': ['current', 2],             # A
    'odo':              ['odometer', 2],            # km
}

API_URL = "https://api.iternio.com/1/tlm"

class Abrp():
    """ State wrapper """

    def __init__(self, settings, api_key):
        log.info('Initializing ABRP')
        self._api_key = api_key
        self._token = settings['token']
        self._interval = settings.get('interval', 5)
        self._next_transmit = 0
        self._session = ClientSession()
        self._last_data = {}

    async def transmit(self, fields):
        now = monotonic()

        if now >= self._next_transmit and 'speed' in fields:
            payload = {
                    'utc': int(fields['timestamp']),
                    'power': 0,
                    'current': 0,
                    }
            payload.update({v[0]: round(fields[k], v[1]) for k, v in PID_MAP.items()
                            if k in fields and fields[k] is not None})

            payload['speed'] *= 3.6
            if 'capacity' in payload:
                payload['capacity'] /= 1000  # Wh -> kWh

            try:
                json={'api_key': self._api_key,
                        'token': self._token,
                        'tlm': payload}
                log.debug(f'Send payload {json}')
                ret = await self._session.post(API_URL + "/send", json=json)
                async with ret:
                    if ret.content_type == 'application/json':
                        ret_json = await ret.json()
                        status = ret_json['status']
                    else:
                        status = await ret.text()

                    if ret.status != 200 or status != "ok":
                        log.error(f'Submit error: {dumps(json)} {status=} {ret.reason=}')
                    else:
                        log.info(f'Post result: {ret.status=} {ret.reason=}')

            except ClientConnectionError as err:
                log.error(f'ClientConnectionError: {err}')
            except ContentTypeError as err:
                async with ret:
                    text = await ret.text()
                    log.error(f'ContentTypeError: {err} {ret.content_type=} {text=}')

            self._next_transmit = now + self._interval
