import logging
import aiomqtt

log = logging.getLogger('MQTT')


class Mqtt():
    """ State wrapper """

    def __init__(self, settings, carid):
        self._mqtt_server = settings['server']
        self._mqtt_port = settings['port']
        self._mqtt_user = settings['user']
        self._mqtt_pass = settings['pass']
        self._carid = carid
        self._last_soc = None

    async def transmit(self, dataset):
        carid = self._carid

        for fields in dataset:
            if 'SOC_DISPLAY' in fields:
                self._last_soc = fields['SOC_DISPLAY']

        if self._last_soc is not None:
            async with aiomqtt.Client(hostname=self._mqtt_server,
                                      port=self._mqtt_port,
                                      username=self._mqtt_user,
                                      password=self._mqtt_pass) as client:
                await client.publish(f'Car/{carid}/SOC',
                                     payload=float(self._last_soc))

    @staticmethod
    def which_fields():
        """ Return set of fields this module likes to use """
        return set(['SOC_DISPLAY'])
