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
        self._last_fields = {}
        self._last_soc_d = None
        self._last_soc_b = None

    async def transmit(self, dataset):
        """ Forward data to EVNotify """
        data = dataset[-1]
        evn = self._evn
        fields = self._last_fields
        fields.update({key: value for key, value in data.items()
                       if key in EXTENDED_FIELDS})
        if 'SOC_DISPLAY' in data:
            self._last_soc_d = data['SOC_DISPLAY']
        if 'SOC_BMS' in data:
            self._last_soc_b = data['SOC_BMS']

        now = monotonic()

        if now >= self._next_transmit:
            soc_display = self._last_soc_d
            soc_bms = self._last_soc_b
            try:
                extended_data = {name: round(value, EXTENDED_FIELDS[name])
                                 for name, value in fields.items()
                                 if value is not None}
                await evn.setSOC(soc_display, soc_bms)
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

    @staticmethod
    def which_fields():
        """ Return set of fields this module likes to use """
        return set(('SOC_DISPLAY', 'SOC_BMS')) | set(EXTENDED_FIELDS.keys())
