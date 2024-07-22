import logging
import aiomqtt

log = logging.getLogger('MQTT')


class Mqtt():
    """ State wrapper """

    def __init__(self, settings, carid):
        self._mqtt_client = aiomqtt.Client(hostname=settings['server'],
                                           port=settings['port'],
                                           username=settings['user'],
                                           password=settings['pass'])
        self._carid = carid
        self._last_soc = None

    async def close(self):
        pass

    async def transmit(self, dataset):
        carid = self._carid

        for fields in dataset:
            if 'SOC_DISPLAY' in fields:
                self._last_soc = fields['SOC_DISPLAY']

        if self._last_soc is not None:
            try:
                async with self._mqtt_client as client:
                    await client.publish(f'Car/{carid}/SOC',
                                         payload=float(self._last_soc))
            except aiomqtt.error.MqttError as e:
                log.warning("MqttError(%s)", str(e))

    @staticmethod
    def which_fields():
        """ Return set of fields this module likes to use """
        return set(['SOC_DISPLAY'])
