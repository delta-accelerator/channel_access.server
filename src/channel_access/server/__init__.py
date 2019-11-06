import threading
import weakref
from datetime import datetime, timedelta, timezone

import channel_access.common as ca
from . import cas
from .cas import ExistsResponse, AttachResponse

# Only import numpy if compiled with numpy support
if cas.NUMPY_SUPPORT:
    import numpy
else:
    numpy = None


try:
    from math import isclose as _isclose
except ImportError:
    # implement our own according to the python source
    def _isclose(a, b, *, rel_tol=1e-09, abs_tol=0.0):
        import math

        if rel_tol < 0.0 or abs_tol < 0.0:
            raise ValueError('tolerances must be non-negative')
        if a == b:
            return True
        if math.isinf(a) or math.isinf(b):
            return False
        diff = abs(a - b)
        return diff <= abs(rel_tol * b) or diff <= abs(rel_tol * a) or diff < abs_tol


def is_sequence(value):
    """
    Return ``True`` if ``value`` is a sequence type.

    Sequences are types which can be iterated over but are not strings
    or bytes.
    """
    return not isinstance(value, str) and not isinstance(value, bytes) and hasattr(type(value), '__iter__')


def default_attributes(type_, count=None, use_numpy=None):
    """
    Return the default attributes dictionary for new PVs.

    Args:
        type (:class:`channel_access.common.Type`): Type of the PV.
        count (int): Number of elements of the array PV or ``None`` if scalar.
        use_numpy (bool): If ``True`` use numpy arrays.

    Returns:
        dict: Attributes dictionary.
    """
    if use_numpy is None:
        use_numpy = numpy is not None

    result = {
        'status': ca.Status.UDF,
        'severity': ca.Severity.INVALID,
        'timestamp': datetime.now(timezone.utc)
    }

    if type_ == ca.Type.STRING:
        result['value'] = ''
    else:
        if count is None:
            result['value'] = 0
        else:
            if numpy and use_numpy:
                result['value'] = numpy.zeros(count)
            else:
                result['value'] = (0,) * count
        result['unit'] = ''
        result['control_limits'] = (0, 0)
        result['display_limits'] = (0, 0)
        result['alarm_limits'] = (0, 0)
        result['warning_limits'] = (0, 0)
        if type_ == ca.Type.FLOAT or type_ == ca.Type.DOUBLE:
            result['precision'] = 0
        if type_ == ca.Type.ENUM:
            result['enum_strings'] = ('',)

    return result


def failing_write_handler(pv, value, timestamp, context):
    """
    A write handler which disallows writes.
    """
    return False


class AsyncRead(cas.AsyncRead):
    """
    Asyncronous read completion class.

    Create an object of this class in a read handler and return it
    to signal an asynchronous read operation.

    When the read operation is completed call the :meth:`complete`
    method. If it fails call the :meth:`fail` method.
    """
    def __init__(self, pv, context):
        super().__init__(context)
        self._pv = pv

    def complete(self, attributes):
        """
        Complete the asynchronous read operation.

        This updates the PV object and signals the completion to the
        server.

        This method is thread-safe.

        Args:
            attributes (dict): An attributes dictionary with the read
                attributes.
        """
        pv = self._pv
        with pv._attributes_lock:
            pv._update_attributes(attributes)
            attributes = pv._copy_attributes()
        super().complete(pv._pv._encode(attributes))

    def fail(self):
        """
        Fail the asynchronous read operation.

        This signals a failure of the read operation to the server.

        This method is thread-safe.
        """
        super().fail()


class AsyncWrite(cas.AsyncWrite):
    """
    Asyncronous write completion class.

    Create an object of this class in a write handler and return it
    to signal an asynchronous write operation.

    When the write operation is completed call the :meth:`complete`
    method. If it fails call the :meth:`fail` method.
    """
    def __init__(self, pv, context):
        super().__init__(context)
        self._pv = pv

    def complete(self, value, timestamp=None):
        """
        Complete the asynchronous write operation.

        This updates the PV object and signals the completion to the
        server.

        This method is thread-safe.

        Args:
            value: The new value for the *value* attribute.
            timestamp (datetime): The new value for the *timestamp* attribute.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        self._pv._update_value_timestamp(value, timestamp)
        super().complete()

    def fail(self):
        """
        Fail the asynchronous write operation.

        This signals a failure of the write operation to the server.

        This method is thread-safe.
        """
        super().fail()


class PV(object):
    """
    A channel access PV.

    This class gives thread-safe access to a channel access PV.
    Always create PV objects through :meth:`Server.createPV()`.

    The following keys can occur in an attributes dictionary:

    value
        Data value, type depends on the PV type. For integer types
        and enum types this is ``int``, for floating point types
        this is ``float``. For string types this is ``bytes`` or ``str``
        depending on the ``encondig`` parameter.
        For arrays this is a sequence of the corresponding values.

    status
        Value status, one of :class:`channel_access.common.Status`.

    severity
        Value severity, one of :class:`channel_access.common.Severity`.

    timestamp
        An aware datetime representing the point in time the value was
        changed.

    enum_strings
        Tuple with the strings corresponding to the enumeration values.
        The length of the tuple must be equal to the maximum allowed
        value + 1.
        The entries are ``bytes`` or ``str`` depending on the
        ``encondig`` parameter.
        This is only used for enum PVs.

    unit
        String representing the physical unit of the value. The type is
        ``bytes`` or ``str`` depending on the ``encondig`` parameter.
        This is only used for numerical PVs.

    precision
        Integer representing the number of relevant decimal places.
        This is only used for floating point PVs.

    display_limits
        A tuple ``(minimum, maximum)`` representing the range of values
        for a user interface.
        This is only used for numerical PVs.

    control_limits
        A tuple ``(minimum, maximum)`` representing the range of values
        accepted for a put request by the server.
        This is only used for numerical PVs.

    warning_limits
        A tuple ``(minimum, maximum)``. When any value lies outside of the
        range the status becomes :class:`channel_access.common.Status.LOW` or :class:`channel_access.common.Status.HIGH`.
        This is only used for numerical types.

    alarm_limits
        A tuple ``(minimum, maximum)``. When any value lies outside of the
        range the status becomes :class:`channel_access.common.Status.LOLO` or :class:`channel_access.common.Status.HIHI`.
        This is only used for numerical PVs.

    A read handler allows to customize the retrievel of attribute values
    via channel access and perform asynchronous reads.
    It is called from an unspecified thread and should not block.

        **Signature**: ``read_handler(pv, context)``

        **Parameters**:

            * **pv** (:class:`PV`): The :class:`PV` which attributes are requested.
            * **context** : A context object needed to create an :class:`AsyncRead` object.

        **Returns**:
            * ``True`` to allow the read and use the current attributes.
            * ``False`` to disallow the read.
            * An attributes dictionary to update the attributes and use them.
            * An :class:`AsyncRead` object to signal an asynchronous read operation.

    A write handler allows to customize the change of the PV value
    via channel access and perform asynchronous writes.
    It is called from an unspecified thread and should not block.

        **Signature**: ``write_handler(pv, value, timestamp, context)``

        **Parameters**:

            * **pv** (:class:`PV`): The :class:`PV` which value is changed.
            * **value**: The new value for the *value* attribute.
            * **timestmap**: The new value for the *timestamp* attribute.
            * **context** : A context object needed to create an :class:`AsyncWrite` object.

        **Returns**:
            * ``True`` to allow the write and update the attributes
            * ``False`` to disallow the write.
            * A tuple ``(value, timestamp)`` to use instead of the arguments.
            * An :class:`AsyncWrite` object to signal an asynchronous write operation.
    """
    def __init__(self, name, type_, *, count=None, attributes=None,
            value_deadband=0, archive_deadband=0,
            read_handler=None, write_handler=None, read_only=False,
            encoding='utf-8', monitor=None, use_numpy=None):
        """
        Args:
            name (str|bytes): Name of the PV.
                If ``encoding`` is ``None`` this must be raw bytes.
            type (:class:`channel_access.common.Type`): The PV type.
            count (int): The initial array length. Use ``None`` for a scalar PV.
            attributes (dict): Attributes dictionary with the initial attributes.
                These will override the default attributes.
            value_deadband (int|float): If any value changes more than this
                deadband a value event is fired.
                This is only used for numerical PVs.
            archive_deadband (int|float): If any value changes more than this
                deadband an archive event is fired.
                This is only used for numerical PVs.
            read_handler (callable): A callable used as the read handler
                for read requests from a client.
            write_handler (callable): A callable used as the write handler
                for write requests from a client.
            read_only (bool): If ``True`` the value can't be changed via
                channel access puts. A custom write handler will overwrite
                this setting.
            monitor (callable):
                This is the initial value for the monitor handler.
            encoding (str): The encoding used for the PV name and string
                attributes. If ``None`` these values must be bytes.
            use_numpy (bool): If ``True`` use numpy arrays. If ``None``
                use numpy arrays if numpy support is available.
        """
        super().__init__()
        if use_numpy is None:
            use_numpy = numpy is not None
        if read_only and not write_handler:
            write_handler = failing_write_handler
        self._pv = _PV(name, self, use_numpy=use_numpy, encoding=encoding,
            read_handler=read_handler, write_handler=write_handler)

        self._name = name
        self._type = type_
        self._value_deadband = value_deadband
        self._archive_deadband = archive_deadband
        # Used for float comparisons
        self._relative_tolerance = 1e-05
        self._absolute_tolerance = 1e-08

        self._attributes_lock = threading.Lock()
        self._monitor_handler = monitor
        self._outstanding_events = ca.Events.NONE
        self._publish_events = False
        self._attributes = default_attributes(type_, count, use_numpy)

        if attributes is not None:
            self._update_attributes(attributes)

    # only call with attributes lock held
    def _update_status_severity(self, status, severity):
        """ Update the status and serverity. """
        changed = False
        if status != self._attributes.get('status'):
            self._attributes['status'] = status
            changed = True
        if severity != self._attributes.get('severity'):
            self._attributes['severity'] = severity
            changed = True
        if changed:
            self._outstanding_events |= ca.Events.ALARM

    # only call with attributes lock held
    def _constrain_value(self, value):
        """ Constrain a value to the control limits range. """
        if self._type != ca.Type.STRING:
            ctrl_limits = self._attributes.get('control_limits')

            if ctrl_limits is not None and ctrl_limits[0] < ctrl_limits[1]:
                clamp = lambda v: max(min(v, ctrl_limits[1]), ctrl_limits[0])
                if is_sequence(value):
                    return tuple(map(clamp, value))
                else:
                    return clamp(value)
        return value

    # only call with attributes lock held
    def _calculate_status_severity(self, value):
        """ Calculate status and severity values using warning and alarm limits. """
        status = ca.Status.NO_ALARM
        severity = ca.Severity.NO_ALARM
        if self._type != ca.Type.STRING:
            alarm_limits = self._attributes.get('alarm_limits')
            warn_limits = self._attributes.get('warning_limits')

            if is_sequence(value):
                # For arrays use the extreme values
                lowest = min(value)
                highest = max(value)
            else:
                lowest = value
                highest = value

            if warn_limits is not None and warn_limits[0] < warn_limits[1]:
                if lowest < warn_limits[0] and highest > warn_limits[1]:
                    # If both limits are violated (can happen in arrays)
                    # the violation with the highest absolute difference
                    # is used
                    if abs(lowest - warn_limits[0]) > abs(highest - warn_limits[1]):
                        severity = ca.Severity.MINOR
                        status = ca.Status.LOW
                    else:
                        severity = ca.Severity.MINOR
                        status = ca.Status.HIGH
                elif lowest < warn_limits[0]:
                    severity = ca.Severity.MINOR
                    status = ca.Status.LOW
                elif highest > warn_limits[1]:
                    severity = ca.Severity.MINOR
                    status = ca.Status.HIGH

            if alarm_limits is not None and alarm_limits[0] < alarm_limits[1]:
                if lowest < alarm_limits[0] and highest > alarm_limits[1]:
                    # If both limits are violated (can happen in arrays)
                    # the violation with the highest absolute difference
                    # is used
                    if abs(lowest - alarm_limits[0]) > abs(highest - alarm_limits[1]):
                        severity = ca.Severity.MAJOR
                        status = ca.Status.LOLO
                    else:
                        severity = ca.Severity.MAJOR
                        status = ca.Status.HIHI
                elif lowest < alarm_limits[0]:
                    severity = ca.Severity.MAJOR
                    status = ca.Status.LOLO
                elif highest > alarm_limits[1]:
                    severity = ca.Severity.MAJOR
                    status = ca.Status.HIHI
        return status, severity

    # only call with attributes lock held
    def _update_value(self, value):
        """ Update the value and depending on it the status and severity. """
        value = self._constrain_value(value)
        status, severity = self._calculate_status_severity(value)

        old_value = self._attributes.get('value')
        if is_sequence(value) != is_sequence(old_value):
            # If old and new_value differ in wether they are sequences or not
            # we can't compare them.
            value_changed = True
        elif is_sequence(value) and len(value) != len(old_value):
            # If the lenth of both sequences are different we can't
            # compare them either.
            value_changed = True
        else:
            if self._type in (ca.Type.FLOAT, ca.Type.DOUBLE):
                # Floating point types can't be compare with the equal
                # operator because of rounding errors
                isclose = lambda a, b: _isclose(a, b, rel_tol=self._relative_tolerance, abs_tol=self._absolute_tolerance)
                if is_sequence(value):
                    # sequences are compared element-wise
                    if numpy and (isinstance(value, numpy.ndarray) or isinstance(old_value, numpy.ndarray)):
                        value_changed = not numpy.allclose(value, old_value, rtol=self._relative_tolerance, atol=self._absolute_tolerance)
                    else:
                        value_changed = not all(map(lambda x: isclose(x[0], x[1]), zip(value, old_value)))
                else:
                    value_changed = not isclose(value, old_value)
            else:
                # Integer types are compared with the equal operator
                if numpy and (isinstance(value, numpy.ndarray) or isinstance(old_value, numpy.ndarray)):
                    value_changed = not numpy.all(numpy.equal(value, old_value))
                else:
                    # For simple sequences this compares element-wise
                    value_changed = value != old_value

        if value_changed:
            self._attributes['value'] = value

            # Deadbands are only defined for number types
            check_deadbands = self._type not in (ca.Type.STRING, ca.Type.ENUM)
            if is_sequence(value) != is_sequence(old_value):
                # If the values differ in wether they are sequences or not,
                # deadbands can't be used.
                check_deadbands = False
            elif is_sequence(value) and len(value) != len(old_value):
                # If the lenth of both sequences are different we can't
                # use the deadbands either.
                check_deadbands = False

            if check_deadbands:
                if is_sequence(value):
                    # Look at the maximum difference between the old values
                    # and the new ones.
                    diff = max(map(lambda x: abs(x[0] - x[1]), zip(value, old_value)))
                else:
                    diff = abs(value - old_value)
                if diff >= self._value_deadband:
                    self._outstanding_events |= ca.Events.VALUE
                if diff >= self._archive_deadband:
                    self._outstanding_events |= ca.Events.ARCHIVE
            else:
                self._outstanding_events |= ca.Events.VALUE | ca.Events.ARCHIVE

        self._update_status_severity(status, severity)

    # only call with attributes lock held
    def _update_meta(self, key, value):
        """ Update the meta data attributes. """
        if value != self._attributes.get(key):
            self._attributes[key] = value
            self._outstanding_events |= ca.Events.PROPERTY
        if key.endswith('_limits'):
            # If the limits change we might need to change the value accordingly
            self._update_value(self._attributes.get('value'))

    # only call with attributes lock held
    def _update_attributes(self, attributes):
        """ Update attributes using an attributes dictionary. """
        limits_changed = False
        for key in ['timestamp', 'precision', 'enum_strings', 'unit', 'control_limits', 'display_limits', 'alarm_limits', 'warning_limits']:
            if key in attributes and attributes[key] != self._attributes.get(key):
                self._attributes[key] = attributes[key]
                self._outstanding_events |= ca.Events.PROPERTY
                if key.endswith('_limits'):
                    limits_changed = True

        # Change status and serverity first beacuse a value change might
        # override them
        if 'status' in attributes or 'severity' in attributes:
            if 'status' in attributes:
                status = attributes['status']
            else:
                status = self._attributes.get('status')
            if 'severity' in attributes:
                severity = attributes['severity']
            else:
                severity = self._attributes.get('severity')
            self._update_status_severity(status, severity)

        if 'value' in attributes:
            self._update_value(attributes['value'])
        elif limits_changed:
            # If the limits change we might need to change the value accordingly
            self._update_value(self._attributes.get('value'))

    # only call with attributes lock held
    def _publish(self):
        """ Post events if necessary. """
        events = self._outstanding_events
        publish_events = self._publish_events
        monitor_handler = self._monitor_handler
        self._outstanding_events = ca.Events.NONE
        if events != ca.Events.NONE:
            # We need a copy here for thread-safety. This method can
            # be called concurrently multiple times and because we
            # release the lock when posting the atomicity of this
            # call is not ensured without a copy.
            attributes = self._copy_attributes()

            # Release attributes lock during calls to prevent deadlock
            # when a method which changes the attributes is called.
            self._attributes_lock.release()
            try:
                if publish_events:
                    self._pv.postEvents(events, attributes)
                if monitor_handler:
                    monitor_handler(self, attributes)
            finally:
                self._attributes_lock.acquire()

    # only call with attributes lock held
    def _copy_attributes(self):
        # All keys and values are immutable so a shallow copy is enough.
        attributes = self._attributes.copy()
        # If the value is a numpy array whe have to create a copy
        # because numpy arrays are not immutable.
        value = attributes.get('value')
        if numpy and isinstance(value, numpy.ndarray):
            attributes['value'] = numpy.copy(value)
        return attributes

    # only call with attributes lock held
    def _copy_value(self):
        value = self._attributes.get('value')
        # If the value is a numpy array whe have to create a copy
        # because numpy arrays are not immutable.
        if numpy and isinstance(value, numpy.ndarray):
            value = numpy.copy(value)
        return value

    def _set_publish_events(self, value):
        with self._attributes_lock:
            self._publish_events = value

    def _update_value_timestamp(self, value, timestamp):
        with self._attributes_lock:
            self._update_value(value)
            self._update_meta('timestamp', timestamp)
            self._publish()

    @property
    def name(self):
        """
        str: The name of this PV.
        """
        return self._name

    @property
    def use_numpy(self):
        """
        bool: Wether this PV uses numpy arrays for its value.
        """
        return self._pv.use_numpy

    @property
    def is_array(self):
        """
        bool: Wether this PV is an array.
        """
        with self._attributes_lock:
            value = self._attributes.get('value')
        return is_sequence(value)

    @property
    def count(self):
        """
        int: The number of array elements of this PV or ``None`` for a scalar.
        """
        with self._attributes_lock:
            value = self._attributes.get('value')
        if is_sequence(value):
            return len(value)
        else:
            return None

    @property
    def type(self):
        """
        :class:`channel_access.common.Type`: The type of this PV.
        """
        return self._type

    @property
    def is_enum(self):
        """
        bool: Wether this PV is of enumeration type.
        """
        return self._type == ca.Type.ENUM

    @property
    def monitor_handler(self):
        """
        callable: The monitor handler.

        The monitor handler is called when the attributes change.
        The handler might be called from an unspecified thread and must be
        thread-safe. It should also not block.

        This is writeable and changes the monitor handler. Set it to
        ``None`` to disable the handler.

        **Signature**: ``fn(pv, attributes)``

        **Parameters**:

            * **pv** (:class:`PV`): The :class:`PV` object with the
              changed values.
            * **attributes** (dict): A attributes dictionary with the new attributes.
        """
        with self._attributes_lock:
            return self._monitor_handler

    @monitor_handler.setter
    def monitor_handler(self, handler):
        with self._attributes_lock:
            self._monitor_handler = handler

    @property
    def attributes(self):
        """
        dict: The current attributes dictionary

        This is writeable and updates the attributes dictionary
        """
        with self._attributes_lock:
            return self._copy_attributes()

    @attributes.setter
    def attributes(self, attributes):
        with self._attributes_lock:
            self._update_attributes(attributes)

    @property
    def timestamp(self):
        """
        datetime: The timestamp in UTC of the last time value has changed.
        """
        with self._attributes_lock:
            return self._attributes.get('timestamp')

    @property
    def value(self):
        """
        The current value of the PV.

        This is writeable and updates the value and timestamp.

        Wether the PV is an array and the size of the array is dependent
        on the type of the value attribute.
        """
        with self._attributes_lock:
            return self._copy_value()

    @value.setter
    def value(self, value):
        self._update_value_timestamp(value, datetime.now(timezone.utc))

    @property
    def value_timestamp(self):
        with self._attributes_lock:
            timestamp = self._attributes.get('timestamp')
            value = self._copy_value()
        return (value, timestamp)

    @property
    def status(self):
        """
        :class:`channel_access.common.Status`: The current status.

        This is writeable and updates the status.
        """
        with self._attributes_lock:
            return self._attributes.get('status')

    @status.setter
    def status(self, value):
        with self._attributes_lock:
            self._update_status_severity(value, self._attributes.get('severity'))
            self._publish()

    @property
    def severity(self):
        """
        :class:`channel_access.common.Severity`: The current severity.

        This is writeable and updates the severity.
        """
        with self._attributes_lock:
            return self._attributes.get('severity')

    @severity.setter
    def severity(self, value):
        with self._attributes_lock:
            self._update_status_severity(self._attributes.get('status'), value)
            self._publish()

    @property
    def status_severity(self):
        """
        tuple(:class:`channel_access.common.Status`, :class:`channel_access.common.Severity`): The current status and severity.

        This is writeable and updates the status and severity at
        the same time.
        """
        with self._attributes_lock:
            return (self._attributes.get('status'), self._attributes.get('severity'))

    @status_severity.setter
    def status_severity(self, value):
        with self._attributes_lock:
            self._update_status_severity(value[0], value[1])
            self._publish()

    @property
    def precision(self):
        """
        int: The current precision.

        This is writeable and update the precision.
        """
        with self._attributes_lock:
            return self._attributes.get('precision')

    @precision.setter
    def precision(self, value):
        with self._attributes_lock:
            self._update_meta('precision', value)
            self._publish()

    @property
    def unit(self):
        """
        str|bytes: The current unit.

        The type depends on the ``encoding`` parameter.

        This is writeable and updates the unit.
        """
        with self._attributes_lock:
            return self._attributes.get('unit')

    @unit.setter
    def unit(self, value):
        with self._attributes_lock:
            self._update_meta('unit', value)
            self._publish()

    @property
    def enum_strings(self):
        """
        tuple(str|bytes): The current enumeration strings.

        The type depends on the ``encoding`` parameter.

        This is writeable and updates the enumeration strings. The length
        of the tuple must be equal to the maximum value + 1.
        """
        with self._attributes_lock:
            return self._attributes.get('enum_strings')

    @enum_strings.setter
    def enum_strings(self, value):
        assert(len(value) <= 16)
        with self._attributes_lock:
            self._update_meta('enum_strings', value)
            self._publish()

    @property
    def display_limits(self):
        """
        tuple(int|float, int|float): The current display limits.

        This is writeable and updates the display limits:
        """
        with self._attributes_lock:
            return self._attributes.get('display_limits')

    @display_limits.setter
    def display_limits(self, value):
        assert(len(value) == 2)
        with self._attributes_lock:
            self._update_meta('display_limits', value)
            self._publish()

    @property
    def control_limits(self):
        """
        tuple(int|float, int|float): The control display limits.

        This is writeable and updates the control display limits.
        """
        with self._attributes_lock:
            return self._attributes.get('control_limits')

    @control_limits.setter
    def control_limits(self, value):
        assert(len(value) == 2)
        with self._attributes_lock:
            self._update_meta('control_limits', value)
            self._publish()

    @property
    def warning_limits(self):
        """
        tuple(int|float, int|float): The warning display limits.

        This is writeable and updates the warning limits.
        """
        with self._attributes_lock:
            return self._attributes.get('warning_limits')

    @warning_limits.setter
    def warning_limits(self, value):
        assert(len(value) == 2)
        with self._attributes_lock:
            self._update_meta('warning_limits', value)
            self._publish()

    @property
    def alarm_limits(self):
        """
        tuple(int|float, int|float): The alarm display limits.

        This is writeable and updates the alarm limits.
        """
        with self._attributes_lock:
            return self._attributes.get('alarm_limits')

    @alarm_limits.setter
    def alarm_limits(self, value):
        assert(len(value) == 2)
        with self._attributes_lock:
            self._update_meta('alarm_limits', value)
            self._publish()


class _PV(cas.PV):
    """
    cas.PV implementation.
    """
    def __init__(self, name, pv, *, use_numpy, encoding, read_handler, write_handler):
        if encoding is not None:
            name = name.encode(encoding)
        super().__init__(name, use_numpy)
        self._pv = pv
        self._encoding = encoding
        self._read_handler = read_handler
        self._write_handler = write_handler

    def _encode(self, attributes):
        """ Convert a high-level attributes dictionary to a low-level one. """
        if self._encoding is not None:
            if 'unit' in attributes:
                attributes['unit'] = attributes['unit'].encode(self._encoding)

            if 'enum_strings' in attributes:
                attributes['enum_strings'] = tuple(x.encode(self._encoding) for x in attributes['enum_strings'])

            if 'value' in attributes and self._pv.type == ca.Type.STRING:
                attributes['value'] = attributes['value'].encode(self._encoding)

        if 'timestamp' in attributes:
            attributes['timestamp'] = ca.datetime_to_epics(attributes['timestamp'])

        return attributes

    def _decode(self, value, timestamp=None):
        """ Convert a low-level value and timestamp to high-level ones. """
        if self._encoding is not None and self._pv.type == ca.Type.STRING:
            value = value.decode(self._encoding)

        if timestamp is not None:
            timestamp = ca.epics_to_datetime(timestamp)

        return value, timestamp

    def count(self):
        return self._pv.count

    def type(self):
        return self._pv.type

    def read(self, context):
        attributes = None
        if self._read_handler:
            result = self._read_handler(self._pv, context)
            if isinstance(result, AsyncRead) or not result:
                return result
            # Test for the True singleton
            if result is not True:
                with self._pv._attributes_lock:
                    self._pv._update_attributes(result)
                    attributes = self._pv._copy_attributes()

        if not attributes:
            attributes = self._pv.attributes
        return self._encode(attributes)

    def write(self, value, timestamp, context):
        value, timestamp = self._decode(value, timestamp)

        if self._write_handler:
            result = self._write_handler(self._pv, value, timestamp, context)

            if isinstance(result, AsyncWrite) or not result:
                return result
            # Test for the True singleton
            if result is not True:
                value, timestamp = result

        try:
            self._pv._update_value_timestamp(value, timestamp)
        except:
            return False
        else:
            return True

    def interestRegister(self):
        self._pv._set_publish_events(True)
        return True

    def interestDelete(self):
        self._pv._set_publish_events(False)

    def postEvents(self, events, attributes):
        self.postEvent(events, self._encode(attributes))


_sentinal = object()

class Server(object):
    """
    Channel access server.

    On creation this class creates a server thread which processes
    channel access messages.

    The :meth:`shutdown()` method must be called to stop the server.

    This class implements the context manager protocol. This automatically
    shuts the server down at the end of the with-statement::

        with cas.Server() as server:
            pass
    """
    def __init__(self, *, encoding=None, use_numpy=None):
        """
        Args:
            encoding (str): If not ``None`` this value is used as a
                default for the ``encoding`` parameter when
                calling :meth:`createPV`.
            use_numpy (bool): If not ``None`` this value is used as a
                default for the ``use_numpy`` parameter when
                calling :meth:`createPV`.
        """
        super().__init__()
        self._encoding = encoding
        self._use_numpy = use_numpy
        self._server = _Server(self)
        self._thread = _ServerThread()

        self._pvs_lock = threading.Lock()
        self._pvs = weakref.WeakValueDictionary()
        self._encoded_pvs = weakref.WeakValueDictionary()
        self._aliases = {}
        self._encoded_aliases = {}
        self._alias_to_encoded = {}

        self._thread.start()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

    @property
    def pvs(self):
        """
        Return a list of all active PV objects.

        The list contains all PV objects which are alive and not garbage
        collected.

        This property is thread-safe.

        Returns:
          list(:class:`PV`): List of active PV objects.
        """
        with self._pvs_lock:
            return list(self._pvs.values())

    @property
    def aliases(self):
        """
        Return a dict of all aliases.

        This property is thread-safe.

        Returns:
            dict(alias: name): Alias mappings
        """
        with self._pvs_lock:
            return self._aliases.copy()

    def shutdown(self):
        """
        Shutdown the channel access server.

        After this is called no other methods can be called.
        """
        self._thread.stop()
        self._thread.join()
        self._server = None

    def createPV(self, *args, **kwargs):
        """
        Create a new channel access PV.

        All arguments are forwarded to the :class:`PV` class.

        The server does not hold a reference to the returned PV so it
        can be collected if it is no longer used. It is the  responsibility
        of the user to keep the PV objects alive as long as they are needed.

        If a PV with an already existing name is created the server will
        use the new PV and ignore the other one. Connections made to the
        old PV remain active and use the old PV object.

        This method is thread-safe.

        Returns:
            :class:`PV`: A new PV object.
        """
        if 'encoding' not in kwargs and self._encoding is not None:
            kwargs['encoding'] = self._encoding
        if 'use_numpy' not in kwargs and self._use_numpy is not None:
            kwargs['use_numpy'] = self._use_numpy

        pv = PV(*args, **kwargs)
        with self._pvs_lock:
            self._pvs[pv.name] = pv
            self._encoded_pvs[pv._pv.name()] = pv
        return pv

    def retreivePV(self, name):
        """
        Return the active PV object for ``name``.

        Raises a :class:`KeyError` if no PV object can be found.

        This method is thread-safe.

        Args:
            name (str): Name of the PV.

        Returns:
            :class:`PV`: A new PV object.
        """
        with self._pvs_lock:
            pv = self._pvs.get(name)
            if pv is None:
                name = self._aliases.get(name)
                if name is not None:
                    pv = self._pvs.get(name)

        if pv is None:
            raise KeyError
        return pv

    def addAlias(self, alias, name, *, encoding=_sentinal):
        """
        Add an alternative name for a PV.

        PVs created with :meth:`createPV` are searched first. If none is found
        the alias list ist checked. If an alias exists a second search
        using the original name is made and if a PV exists it is used.

        This method is thread-safe.

        Args:
            alias (str): The alternative name.
            name (str): The original name.
            encoding (str): The encoding used for the names.
                            See *encoding* parameter of :class:`PV` objects.
        """
        if encoding is _sentinal:
            if self._encoding is None:
                encoding = 'utf-8'
            else:
                encoding = self._encoding

        if encoding is not None:
            encoded_alias = alias.encode(encoding)
            encoded_name = name.encode(encoding)
        else:
            encoded_alias = alias
            encoded_name = name

        with self._pvs_lock:
            self._aliases[alias] = name
            self._encoded_aliases[encoded_alias] = encoded_name
            self._alias_to_encoded[alias] = encoded_alias

    def removeAlias(self, alias):
        """
        Remove an alias.

        Args:
            alias (str): The alias to remove
        """
        with self._pvs_lock:
            del self._aliases[alias]
            encoded_alias = self._alias_to_encoded.get(alias)
            del self._alias_to_encoded[alias]
            del self._encoded_aliases[encoded_alias]

    def _get_pv(self, pv_name):
        with self._pvs_lock:
            pv = self._encoded_pvs.get(pv_name)
            if pv is None:
                pv_name = self._encoded_aliases.get(pv_name)
                if pv_name is not None:
                    pv = self._encoded_pvs.get(pv_name)
        return pv


class _Server(cas.Server):
    """
    cas.Server implementation.
    """
    def __init__(self, server):
        super().__init__()
        self._server = server

    def pvExistTest(self, client, pv_name):
        if self._server._get_pv(pv_name) is not None:
            return ExistsResponse.EXISTS_HERE

        return ExistsResponse.NOT_EXISTS_HERE

    def pvAttach(self, pv_name):
        pv = self._server._get_pv(pv_name)
        if pv is not None:
            return pv._pv

        return AttachResponse.NOT_FOUND


class _ServerThread(threading.Thread):
    """
    A thread calling cas.process() until :meth:`stop()` is called.
    """
    def __init__(self):
        super().__init__()
        self._should_stop = threading.Event()

    def run(self):
        while not self._should_stop.is_set():
            cas.process(0.1)

    def stop(self):
        self._should_stop.set()
