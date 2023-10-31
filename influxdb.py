from datetime import datetime, timezone
from time import monotonic
import logging
from influxdb_client import WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import pyrfc3339

log = logging.getLogger('InfluxDB')

INT_FIELD_LIST = ('charging', 'fanFeedback', 'fanStatus', 'fix_mode',
                  'normalChargePort', 'rapidChargePort', 'submit_queue_len')
STR_FIELD_LIST = ('cartype', 'akey', 'gps_device')


class InfluxDB():
    def __init__(self, settings, carid):
        log.info('Initializing InfluxDB')

        self._vehicle_id = carid
        self._bucket = settings['bucket']
        self._influx = InfluxDBClientAsync(
                url=settings['url'],
                org=settings['org'],
                token=settings['token'])
        self._iwrite = self._influx.write_api()
        #self._field_states = {}

    async def transmit(self, dataset):
        now = monotonic()

        points = []
        for data in dataset:
            point = {'measurement': 'telemetry', 'tags': data['tags']}

            fields = {}
            for key, value in data['fields'].items():
                if value is None:
                    continue

                if key in STR_FIELD_LIST:
                    point['tags'][key] = str(value)
                else:
                    fields[key] = int(value) if key in INT_FIELD_LIST else float(value)

            point['time'] = pyrfc3339.generate(datetime.fromtimestamp(
                data['time'], timezone.utc))
            point['fields'] = fields

            points.append(point)

        try:
            log.debug(f'enqueue {points=}')
            await self._iwrite.write(bucket=self._bucket,
                                     record=points)
            points.clear()
        except InfluxDBError as exception:
            log.warning(str(exception))
