from time import monotonic, time
from json import dumps
import logging
from aiohttp import ClientSession, ClientConnectionError, ContentTypeError

log = logging.getLogger('ABRP')

PID_MAP = {
    'timestamp':        ('utc', 0),                 # s
    'SOC_DISPLAY':      ('soc', 1),                 # %
    'dcBatteryPower':   ('power', 2),               # kW
    'speed':            ('speed', 1),               # km/h
    'latitude':         ('lat', 9),                 # °
    'longitude':        ('lon', 9),                 # °
    'charging':         ('is_charging', 0),         # bool 1/0
    'rapidChargePort':  ('is_dcfc', 0),             # bool 1/0
    'isParked':         ('is_parked', 0),           # bool 1/0
    'cumulativeEnergyCharged': ('kwh_charged', 2),  # kWh
    'soh':              ('soh', 0),                 # %
    'heading':          ('heading', 2),             # °
    'altitude':         ('elevation', 1),           # m
    'externalTemperature':   ('ext_temp', 1),       # °C
    'batteryAvgTemperature': ('batt_temp', 1),      # °C
    'dcBatteryVoltage': ('voltage', 2),             # V
    'dcBatteryCurrent': ('current', 2),             # A
    'odo':              ('odometer', 2),            # km
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
        self._last_dataset = {}
        self._submit_queue = []

    async def transmit(self, new_dataset):
        """ forward data to ABRP """
        now = monotonic()
        dataset = self._last_dataset
        queue = self._submit_queue
        for data in new_dataset:
            dataset.update({v[0]: data[k]
                            for k, v in PID_MAP.items() if k in data})
            queue.append(dataset.copy())

        if now >= self._next_transmit and len(queue) > 0:
            payload = {}
            for key, decimals in PID_MAP.values():
                data = [point[key] for point in queue \
                        if key in point and point[key] is not None]
                data_len = len(data)
                if data_len > 0:
                    avg = sum(data) / data_len
                    payload[key] = round(avg, decimals) if decimals > 0 else \
                                   int(round(avg, 0))

            if 'utc' not in payload:
                return

            queue.clear()

            if 'speed' in payload:
                payload['speed'] *= 3.6
            if 'capacity' in payload:
                payload['capacity'] /= 1000  # Wh -> kWh

            try:
                json = {'api_key': self._api_key,
                        'token': self._token,
                        'tlm': payload}
                #log.debug('Send payload %s', json)
                ret = await self._session.post(API_URL + "/send", json=json)
                async with ret:
                    if ret.content_type == 'application/json':
                        ret_json = await ret.json()
                        status = ret_json['status']
                    else:
                        status = (await ret.text())[:50]  # shorten potential cloud-flare response

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

    @staticmethod
    def which_fields():
        """ Return set of fields this module likes to use """
        return set(PID_MAP.keys())
