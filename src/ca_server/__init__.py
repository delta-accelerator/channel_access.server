import threading
import weakref
import math
from datetime import datetime, timedelta

from . import cas
from ca_client import ca

from .cas import ExistsResponse, AttachResponse
from ca_client.ca import Type, Status, Severity, AccessRights, Events



def default_data(type, count):
    result = {
        'status': Status.UDF,
        'severity': Severity.INVALID,
        'timestamp': datetime.utcnow()
    }

    if type == Type.STRING:
        result['value'] = ''
    elif type == Type.ENUM:
        result['value'] = 0
        result['enum_strings'] = ('',)
    else:
        result['value'] = 0 if count == 1 else (0,) * count
        result['unit'] = ''
        result['control_limits'] = (0, 0)
        result['display_limits'] = (0, 0)
        result['alarm_limits'] = (0, 0)
        result['warning_limits'] = (0, 0)
        if type == Type.FLOAT or type == Type.DOUBLE:
            result['precision'] = 0

    return result


class PV(object):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self._pv = PVImpl(*args, **kwargs)

    @property
    def name(self):
        """
        str: The name of this PV.
        """
        return self._pv.name

    @property
    def count(self):
        """
        int: The number of elements of this PV.
        """
        return self._pv.count()

    @property
    def type(self):
        """
        :class:`FieldType`: The data type of this PV.
        """
        return self._pv.type()

    @property
    def is_enum(self):
        """
        bool: Wether this PV is of enumeration type.
        """
        return self._pv.type == Type.ENUM

    @property
    def data(self):
        """
        dict: A dictionary with the current values.
        """
        with self._pv._data_lock:
            # We need a copy here for thread-safety. All keys and values
            # are immutable so a shallow copy is enough
            return self._pv._data.copy()

    def update_data(self, data):
        with self._pv._data_lock:
            self._pv._update_data(data)

    @property
    def timestamp(self):
        """
        datetime: The timestamp in UTC of the last received data or ``None`` if it's unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('timestamp')

    @property
    def value(self):
        """
        The current value of the PV or ``None`` if it's unknown.

        This is writeable and calls ``put(value, block=False)``.
        """
        with self._pv._data_lock:
            return self._pv._data.get('value')

    @value.setter
    def value(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_value(value)
            pv._update_meta('timestamp', datetime.utcnow())
            pv._publish()

    @property
    def status(self):
        """
        :class:`Status`: The current status or ``None`` if it's unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('status')

    @status.setter
    def status(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_status_severity(value, pv._data.get('severity'))
            pv._publish()

    @property
    def severity(self):
        """
        :class:`Severity`: The current severity or ``None`` if it's unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('severity')

    @severity.setter
    def severity(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_status_severity(pv._data.get('status'), value)
            pv._publish()

    def update_status_severity(self, status, severity):
        pv = self._pv
        with pv._data_lock:
            pv._update_status_severity(status, value)
            pv._publish()

    @property
    def precision(self):
        """
        int: The current precision or ``None`` if it's unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('precision')

    @precision.setter
    def precision(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_meta('precision', value)
            pv._publish()

    @property
    def unit(self):
        """
        str|bytes: The current unit or ``None`` if it's unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('unit')

    @unit.setter
    def unit(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_meta('unit', value)
            pv._publish()

    @property
    def enum_strings(self):
        """
        tuple(str|bytes): The current enumeration strings or ``None`` if it's unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('enum_strings')

    @enum_strings.setter
    def enum_strings(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_meta('enum_strings', value)
            pv._publish()

    @property
    def display_limits(self):
        """
        tuple(float, float): The current display limits or ``None`` if they are unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('display_limits')

    @display_limits.setter
    def display_limits(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_meta('display_limits', value)
            pv._publish()

    @property
    def control_limits(self):
        """
        tuple(float, float): The control display limits or ``None`` if they are unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('control_limits')

    @control_limits.setter
    def control_limits(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_meta('control_limits', value)
            pv._publish()

    @property
    def warning_limits(self):
        """
        tuple(float, float): The warning display limits or ``None`` if they are unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('warning_limits')

    @warning_limits.setter
    def warning_limits(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_meta('warning_limits', value)
            pv._publish()

    @property
    def alarm_limits(self):
        """
        tuple(float, float): The alarm display limits or ``None`` if they are unknown.
        """
        with self._pv._data_lock:
            return self._pv._data.get('alarm_limits')

    @alarm_limits.setter
    def alarm_limits(self, value):
        pv = self._pv
        with pv._data_lock:
            pv._update_meta('alarm_limits', value)
            pv._publish()


class PVImpl(cas.PV):
    def __init__(self, name, type, count=1, data=None, value_deadband=0, archive_deadband=0, encoding='utf-8', io_handler=None):
        super().__init__(name)
        self._type = type
        self._count = count
        self._io_handler = io_handler
        self._encoding = encoding
        self._value_deadband = value_deadband
        self._archive_deadband = archive_deadband

        self._data_lock = threading.Lock()
        self._data = default_data(type, count)
        if data is not None:
            self._data.update(data)
        self._outstanding_events = Events.NONE
        self._publish_events = False

    def _encode(self, data):
        result = data.copy()

        if self._encoding is not None:
            if 'unit' in result:
                result['unit'] = result['unit'].encode(self._encoding)

            if 'enum_strings' in result:
                result['enum_strings'] = tuple(x.encode(self._encoding) for x in result['enum_strings'])

            if 'value' in result and isinstance(result['value'], str):
                result['value'] = result['value'].encode(self._encoding)

        if 'timestamp' in result:
            posix = (result['timestamp'] - datetime(1970, 1, 1)) / timedelta(seconds=1)
            frac, sec = math.modf(posix)
            result['timestamp'] = (sec - ca.EPICS_EPOCH, int(frac * 1E9))

        return result

    def _decode(self, value, timestamp=None):
        if self._encoding is not None and isinstance(value, bytes):
            value = value.decode(self._encoding)

        if timestamp is not None:
            timestamp = datetime.utcfromtimestamp(ca.EPICS_EPOCH + timestamp[0] + timestamp[1] * 1E-9)

        return value, timestamp

    # only call with data lock held
    def _constrain_value(self, value):
        if isinstance(value, int) or isinstance(value, float):
            ctrl_limits = self._data.get('control_limits')

            if ctrl_limits is not None and ctrl_limits[0] < ctrl_limits[1]:
                if value < ctrl_limits[0]:
                    return ctrl_limits[0]
                elif value > ctrl_limits[1]:
                    return ctrl_limits[1]
        return value

    # only call with data lock held
    def _calculate_status_severity(self, value):
        status = Status.NO_ALARM
        severity = Severity.NO_ALARM
        if isinstance(value, int) or isinstance(value, float):
            alarm_limits = self._data.get('alarm_limits')
            warn_limits = self._data.get('warning_limits')

            if warn_limits is not None and warn_limits[0] < warn_limits[1]:
                if value < warn_limits[0]:
                    severity = Severity.MINOR
                    status = Status.LOW
                elif value > warn_limits[1]:
                    severity = Severity.MINOR
                    status = Status.HIGH
            if alarm_limits is not None and alarm_limits[0] < alarm_limits[1]:
                if value < alarm_limits[0]:
                    severity = Severity.MAJOR
                    status = Status.LOLO
                elif value > alarm_limits[1]:
                    severity = Severity.MAJOR
                    status = Status.HIHI
        return status, severity

    # only call with data lock held
    def _update_status_severity(self, status, severity):
        changed = False
        if status != self._data.get('status'):
            self._data['status'] = status
            changed = True
        if severity != self._data.get('severity'):
            self._data['severity'] = severity
            changed = True
        if changed:
            self._outstanding_events |= Events.ALARM

    # only call with data lock held
    def _update_value(self, value):
        value = self._constrain_value(value)
        status, severity = self._calculate_status_severity(value)

        old_value = self._data.get('value')
        if value != old_value:
            self._data['value'] = value
            if isinstance(value, int) or isinstance(value, float):
                diff = abs(value - old_value)
                if diff >= self._value_deadband:
                    self._outstanding_events |= Events.VALUE
                if diff >= self._archive_deadband:
                    self._outstanding_events |= Events.ARCHIVE
            else:
                self._outstanding_events |= Events.VALUE | Events.ARCHIVE
        self._update_status_severity(status, severity)

    # only call with data lock held
    def _update_meta(self, key, value):
        if value != self._data.get(key):
            self._data[key] = value
            self._outstanding_events |= Events.PROPERTY
        if key.endswith('_limits'):
            self._update_value(self._data.get('value'))

    # only call with data lock held
    def _update_data(self, data):
        limits_changed = False
        for key in ['precision', 'enum_strings', 'unit', 'ctrl_limits', 'display_limits', 'alarm_limits', 'warning_limits']:
            if key in data and data[key] != self._data.get(key):
                self._data[key] = data[key]
                self._outstanding_events |= Events.PROPERTY
                if key.endswith('_limits'):
                    limits_changed = True

        if 'status' in data or 'severity' in data:
            if 'status' in data:
                status = data['status']
            else:
                status = self._data.get('status')
            if 'severity' in data:
                severity = data['severity']
            else:
                severity = self._data.get('severity')
            self._update_status_severity(status, severity)

        if 'value' in data:
            self._update_value(data['value'])
        elif limits_changed:
            self._update_value(self._data.get('value'))

    # only call with data lock held
    def _publish(self):
        events = self._outstanding_events
        self._outstanding_events = Events.NONE
        if self._publish_events and events != Events.NONE:
            self.postEvent(events, self._encode(self._data))

    def count(self):
        return self._count

    def type(self):
        return self._type

    def read(self):
        with self._data_lock:
            data = self._data.copy()
        return self._encode(data)

    def write(self, value, timestamp=None):
        value, timestamp = self._decode(value, timestamp)
        with self._data_lock:
            self._update_value(value)
            self._update_meta('timestamp', timestamp)
            self._publish()
            return True
        return False

    def interestRegister(self):
        with self._data_lock:
            self._publish_events = True
        return True

    def interestDelete(self):
        with self._data_lock:
            self._publish_events = False


class Server(object):
    """
    Channel Access server.
    """
    def __init__(self):
        super().__init__()
        self._server = ServerImpl()
        self._thread = ServerThread()

        self._thread.start()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

    def shutdown(self):
        """
        Shutdown the channel access server.
        """
        self._thread.stop()
        self._thread.join()
        self._server = None

    def createPV(self, *args, **kwargs):
        """
        Create a new channel access PV.

        All arguments are forwarded to the :class:`PV` class.

        Returns:
            :class:`PV`: A new PV object.
        """
        return self._server.createPV(*args, **kwargs)


class ServerImpl(cas.Server):
    def __init__(self, encoding='utf-8'):
        super().__init__()
        self._encoding = encoding
        self._pvs = weakref.WeakValueDictionary()

    def pvExistTest(self, client, pv_name):
        if pv_name in self._pvs:
            return ExistsResponse.EXISTS_HERE
        return ExistsResponse.NOT_EXISTS_HERE

    def pvAttach(self, pv_name):
        pv = self._pvs.get(pv_name)
        if pv is None:
            return AttachResponse.NOT_FOUND
        return pv._pv

    def createPV(self, name, *args, **kwargs):
        if self._encoding:
            name = name.encode(self._encoding)
        pv = PV(name, *args, **kwargs)
        self._pvs[name] = pv
        return pv


class ServerThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self._should_stop = threading.Event()

    def run(self):
        while not self._should_stop.is_set():
            cas.process(1.0)

    def stop(self):
        self._should_stop.set()
