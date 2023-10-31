from time import monotonic
import logging
import EVNotifyAPI

EXTENDED_FIELDS = {         # value is decimal places
        'auxBatteryVoltage': 1,
        'batteryInletTemperature': 1,
        'batteryMaxTemperature': 1,
        'batteryMinTemperature': 1,
        'cumulativeEnergyCharged': 1,
        'cumulativeEnergyDischarged': 1,
        'charging': 0,
        'normalChargePort': 0,
        'rapidChargePort': 0,
        'dcBatteryCurrent': 2,
        'dcBatteryPower': 2,
        'dcBatteryVoltage': 2,
        'externalTemperature': 1,
        'odo': 0,
        'soh': 0
        }

log = logging.getLogger('EVNotify')

class EVNotify():
    """ class to keep state """

    def __init__(self, settings):
        log.info('Initilaizing EVNotify')
        self._evn = EVNotifyAPI.EVNotify(settings['akey'], settings['token'])
        self._next_transmit = 0
        self._interval = settings.get('interval', 30)

    async def transmit(self, fields):
        evn = self._evn

        now = monotonic()

        if now >= self._next_transmit and 'SOC_DISPLAY' in fields:
            try:
                extended_data = {name: round(fields[name], decimals)
                                 for name, decimals in EXTENDED_FIELDS.items()
                                 if fields.get(name) is not None}
                await evn.setSOC(fields['SOC_DISPLAY'], fields['SOC_BMS'])
                await evn.setExtended(extended_data)

                is_charging = bool(fields['charging'])
                is_connected = bool(fields['normalChargePort'] or fields['rapidChargePort'])

                if fields['fix_mode'] > 1 and not is_charging and not is_connected:
                    location = {a: fields[a]
                                for a in ('latitude', 'longitude', 'speed')}
                    await evn.setLocation({'location': location})
            except EVNotifyAPI.RateLimit as err:
                log.warning(f'Rate Limited, sleeping 60s {err}')
                self._next_transmit += 60
            except EVNotifyAPI.CommunicationError as err:
                log.error(f'Communication Error: {err}')
            finally:
                self._next_transmit = now + self._interval
