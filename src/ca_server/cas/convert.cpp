#include "convert.hpp"

#include <vector>
#include <unordered_map>
#include <db_access.h>
#include <aitTypes.h>
#include <gddAppTable.h>
#include <gddApps.h>
#include <cadef.h>

#include "cas.hpp"
#include "pv.hpp"

namespace cas {
namespace {

//
// Write functions. Create a python value from a gdd value.
//

PyObject* write_timestamp(gdd const& value)
{
    epicsTimeStamp timestamp = {};
    value.getTimeStamp(&timestamp);

    return Py_BuildValue("(II)", timestamp.secPastEpoch, timestamp.nsec);
}

PyObject* write_string(gdd const& value)
{
    if (value.isScalar()) {
        aitString const* str;
        value.getRef(str);

        return PyBytes_FromStringAndSize(str->string(), str->length());
    }

    return nullptr;
}

template <typename T>
auto py_convert(T value) -> typename std::enable_if<std::is_integral<T>::value, PyObject*>::type
{
    return PyLong_FromLong(value);
}

template <typename T>
auto py_convert(T value) -> typename std::enable_if<std::is_floating_point<T>::value, PyObject*>::type
{
    return PyFloat_FromDouble(value);
}

template <typename T>
PyObject* write_value(gdd const& value)
{
    if (value.isScalar()) {
        return py_convert(static_cast<T>(value));
    } else if (value.isAtomic() and value.dataPointer()) {
        aitIndex first, count;
        if (value.getBound(0, first, count) != 0) return nullptr;

        auto const* data = static_cast<T const*>(value.dataPointer());

        if (count == 1) {
            return py_convert(static_cast<T>(data[0]));
        }

        PyObject* list = PyTuple_New(count);
        if (not list) return nullptr;

        for (aitIndex i = first; i < count; ++i) {
            PyObject* py_val = py_convert(static_cast<T>(data[i]));
            if (not py_val) {
                Py_DECREF(list);
                return nullptr;
            }

            PyTuple_SET_ITEM(list, i - first, py_val);
        }

        return list;
    }

    return nullptr;
}

//
// Read functions. Create a gdd value from a python value.
//
template <typename T>
class ArrayDestructor : public gddDestructor {
public:
    ArrayDestructor(size_t size)
        : array(size)
    {}

    ArrayDestructor(std::vector<T>&& data)
        : array(std::move(data))
    {}

    // data is deleted when this destructor is deleted
    // destructor is always deleted after run is called.
    void run(void*) override
    {}

    std::vector<T> array;
};

template <typename T>
auto py_convert(PyObject* py_val, T& value) -> typename std::enable_if<std::is_integral<T>::value, bool>::type
{
    value = PyLong_AsLong(py_val);
    return not PyErr_Occurred();
}

template <typename T>
auto py_convert(PyObject* py_val, T& value) -> typename std::enable_if<std::is_floating_point<T>::value, bool>::type
{
    value = PyFloat_AsDouble(py_val);
    return not PyErr_Occurred();
}

template <typename T>
bool read_value_impl(PyObject* value, aitEnum type, gdd& result)
{
    if (not PySequence_Check(value)) {
        T val;
        if (not py_convert(value, val)) return false;

        result.setDimension(0, nullptr);
        return result.put(val) == 0;
    }

    Py_ssize_t size = PySequence_Size(value);
    if (PyErr_Occurred()) return false;

    std::vector<T> data(size);
    for (Py_ssize_t i = 0; i < size; ++i) {
        PyObject* item = PySequence_GetItem(value, i);
        if (not item) return false;

        if (not py_convert(item, data[i])) return false;
    }

    if (size == 1) {
        result.setDimension(0, nullptr);
        return result.put(data[0]);
    }

    if (result.dimension() != 1) {
        result.setDimension(1, nullptr);
    }
    result.setBound(0, 0, size);

    ArrayDestructor<T>* destructor = nullptr;
    try {
        destructor = new ArrayDestructor<T>{std::move(data)};
    } catch (...) {
        return false;
    }

    result.putRef(destructor->array.data(), destructor);
    return true;
}

bool to_ait_string(PyObject* value, aitString& string)
{
    if (not value) return false;

    char* str;
    Py_ssize_t size;
    if (PyBytes_AsStringAndSize(value, &str, &size) != 0) return false;

    if (not str) return false;

    // MAX_STRING_SIZE includes the \0 byte?
    if (size > MAX_STRING_SIZE-1) {
        PyErr_Format(PyExc_ValueError, "String length exceeds maximum epics string size of %i bytes", MAX_STRING_SIZE-1);
        return false;
    }

    string.copy(str, size);
    return true;
}

bool read_string(PyObject* value, gdd& result)
{
    aitString ait_str;
    if (not to_ait_string(value, ait_str)) return false;

    return result.put(ait_str) == 0;
}

bool read_status(PyObject* value, gdd& result)
{
    if (not value) return false;

    PyObject* py_val = PyObject_GetAttrString(value, "value");
    if (not py_val) return false;

    int enum_value = PyLong_AsUnsignedLong(py_val);
    Py_DECREF(py_val);
    if (PyErr_Occurred()) return false;

    result.setStat(enum_value);
    return true;
}

bool read_severity(PyObject* value, gdd& result)
{
    if (not value) return false;

    PyObject* py_val = PyObject_GetAttrString(value, "value");
    if (not py_val) return false;

    int enum_value = PyLong_AsUnsignedLong(py_val);
    Py_DECREF(py_val);
    if (PyErr_Occurred()) return false;

    result.setSevr(enum_value);
    return true;
}

bool read_timestamp(PyObject* value, gdd& result)
{
    if (not value) return false;

    PyObject* sec = PyTuple_GetItem(value, 0);
    if (not sec) return false;

    PyObject* nsec = PyTuple_GetItem(value, 1);
    if (not nsec) return false;

    epicsTimeStamp ts;

    ts.secPastEpoch = PyLong_AsLong(sec);
    if (PyErr_Occurred()) return false;

    ts.nsec = PyLong_AsLong(nsec);
    if (PyErr_Occurred()) return false;

    result.setTimeStamp(&ts);
    return true;
}

bool read_value(PyObject* value, aitEnum type, gdd& result)
{
    if (not value) return false;

    switch (type) {
        case aitEnumString:
            return read_string(value, result);
        case aitEnumEnum16:
            return read_value_impl<aitEnum16>(value, type, result);
        case aitEnumInt8:
            return read_value_impl<aitInt8>(value, type, result);
        case aitEnumInt16:
            return read_value_impl<aitInt16>(value, type, result);
        case aitEnumInt32:
            return read_value_impl<aitInt16>(value, type, result);
        case aitEnumFloat32:
            return read_value_impl<aitFloat32>(value, type, result);
        case aitEnumFloat64:
            return read_value_impl<aitFloat64>(value, type, result);
        default:
            PyErr_SetString(PyExc_RuntimeError, "Unhandled gdd type");
            return false;
    }
}

bool read_limits(PyObject* value, aitEnum type, gdd& lower, gdd& upper)
{
    if (not read_value(PyTuple_GetItem(value, 0), type, lower)) return false;
    if (not read_value(PyTuple_GetItem(value, 1), type, upper)) return false;

    return true;
}

bool read_simple(PyObject* dict, aitEnum type, gdd& result)
{
    PyObject* value = PyDict_GetItemString(dict, "value");
    if (not read_value(value, type, result)) return false;

    PyObject* status = PyDict_GetItemString(dict, "status");
    if (not read_status(status, result)) return false;

    PyObject* severity = PyDict_GetItemString(dict, "severity");
    if (not read_severity(severity, result)) return false;

    PyObject* timestamp = PyDict_GetItemString(dict, "timestamp");
    if (not read_timestamp(timestamp, result)) return false;

    return true;
}

bool read_enums(PyObject* dict, gdd& result)
{
    PyObject* enum_strings = PyDict_GetItemString(dict, "enum_strings");
    if (not enum_strings) return false;

    Py_ssize_t size = PyTuple_Size(enum_strings);
    if (PyErr_Occurred()) return false;

    std::vector<aitString> strings(size);
    for (Py_ssize_t i = 0; i < size; ++i) {
        PyObject* item = PyTuple_GetItem(enum_strings, i);

        if (not to_ait_string(item, strings[i])) return false;
    }

    auto* destructor = new ArrayDestructor<aitString>{std::move(strings)};
    result.setDimension(1);
    result.setBound(0, 0, size);
    result.putRef(destructor->array.data(), destructor);
    return true;
}

bool read_enum(PyObject* dict, aitEnum type, gdd& result)
{
    if (not read_simple(dict, type, result[gddAppTypeIndex_dbr_gr_enum_value])) return false;
    if (not read_enums(dict, result[gddAppTypeIndex_dbr_gr_enum_enums])) return false;

    return true;
}

bool read_gr(PyObject* dict, aitEnum type, gdd& result)
{
    if (type == aitEnumFloat32 or type == aitEnumFloat64) {
        if (not read_simple(dict, type, result[gddAppTypeIndex_dbr_gr_double_value])) return false;

        PyObject* precision = PyDict_GetItemString(dict, "precision");
        if (not read_value(precision, aitEnumInt16, result[gddAppTypeIndex_dbr_gr_double_precision])) return false;
    } else {
        if (not read_simple(dict, type, result[gddAppTypeIndex_dbr_gr_long_value])) return false;
    }

    PyObject* unit = PyDict_GetItemString(dict, "unit");
    if (not read_string(unit, result[gddAppTypeIndex_dbr_gr_double_units])) return false;

    PyObject* display_limits = PyDict_GetItemString(dict, "display_limits");
    if (not read_limits(display_limits, type, result[gddAppTypeIndex_dbr_gr_double_graphicLow], result[gddAppTypeIndex_dbr_gr_double_graphicHigh])) return false;

    PyObject* warning_limits = PyDict_GetItemString(dict, "warning_limits");
    if (not read_limits(warning_limits, type, result[gddAppTypeIndex_dbr_gr_double_alarmLowWarning], result[gddAppTypeIndex_dbr_gr_double_alarmHighWarning])) return false;

    PyObject* alarm_limits = PyDict_GetItemString(dict, "alarm_limits");
    if (not read_limits(alarm_limits, type, result[gddAppTypeIndex_dbr_gr_double_alarmLow], result[gddAppTypeIndex_dbr_gr_double_alarmHigh])) return false;

    return true;
}

bool read_ctrl(PyObject* dict, aitEnum type, gdd& result)
{
    if (type == aitEnumFloat32 or type == aitEnumFloat64) {
        if (not read_simple(dict, type, result[gddAppTypeIndex_dbr_ctrl_double_value])) return false;

        PyObject* precision = PyDict_GetItemString(dict, "precision");
        if (not read_value(precision, aitEnumInt16, result[gddAppTypeIndex_dbr_ctrl_double_precision])) return false;
    } else {
        if (not read_simple(dict, type, result[gddAppTypeIndex_dbr_ctrl_long_value])) return false;
    }

    PyObject* unit = PyDict_GetItemString(dict, "unit");
    if (not read_string(unit, result[gddAppTypeIndex_dbr_ctrl_double_units])) return false;

    PyObject* display_limits = PyDict_GetItemString(dict, "display_limits");
    if (not read_limits(display_limits, type, result[gddAppTypeIndex_dbr_ctrl_double_graphicLow], result[gddAppTypeIndex_dbr_ctrl_double_graphicHigh])) return false;

    PyObject* warning_limits = PyDict_GetItemString(dict, "warning_limits");
    if (not read_limits(warning_limits, type, result[gddAppTypeIndex_dbr_ctrl_double_alarmLowWarning], result[gddAppTypeIndex_dbr_ctrl_double_alarmHighWarning])) return false;

    PyObject* alarm_limits = PyDict_GetItemString(dict, "alarm_limits");
    if (not read_limits(alarm_limits, type, result[gddAppTypeIndex_dbr_ctrl_double_alarmLow], result[gddAppTypeIndex_dbr_ctrl_double_alarmHigh])) return false;

    PyObject* control_limits = PyDict_GetItemString(dict, "control_limits");
    if (not read_limits(control_limits, type, result[gddAppTypeIndex_dbr_ctrl_double_controlLow], result[gddAppTypeIndex_dbr_ctrl_double_controlHigh])) return false;

    return true;
}

} // namespace

bool to_exist_return(PyObject* value, pvExistReturn& result)
{
    if (not value) return false;

    switch (PyObject_IsInstance(value, cas::enum_exists)) {
        case 1: {
            PyObject* py_val = PyObject_GetAttrString(value, "value");
            if (not py_val) return false;

            unsigned long enum_value = PyLong_AsUnsignedLong(py_val);
            Py_DECREF(py_val);
            if (PyErr_Occurred()) return false;

            result = pvExistReturn{static_cast<pvExistReturnEnum>(enum_value)};
            return true;
        }
        case 0: {
            PyObject* host_item = PyTuple_GetItem(value, 0);
            if (not host_item) return false;

            unsigned long host = htonl(PyLong_AsUnsignedLong(host_item));
            if (PyErr_Occurred()) return false;

            PyObject* port_item = PyTuple_GetItem(value, 1);
            if (not port_item) return false;

            unsigned long port = htons(PyLong_AsUnsignedLong(port_item));
            if (PyErr_Occurred()) return false;

            caNetAddr addr;
            addr.setSockIP(host, port);
            result = pvExistReturn{addr};
            return true;
        }
    };

    return false;
}

bool to_attach_return(PyObject* value, pvAttachReturn& result)
{
    if (not value) return false;

    switch (PyObject_IsInstance(value, cas::enum_attach)) {
        case 1: {
            PyObject* py_val = PyObject_GetAttrString(value, "value");
            if (not py_val) return false;

            unsigned long enum_value = PyLong_AsUnsignedLong(py_val);
            Py_DECREF(py_val);
            if (PyErr_Occurred()) return false;

            result = pvAttachReturn{static_cast<caStatus>(enum_value)};
            return true;
        }
        case 0: {
            casPV* pv = give_to_server(value);
            if (pv) {
                result = pvAttachReturn{*pv};
            }
            return pv != nullptr;
        }
        default:
            return false;
    };
}

bool to_ait_enum(PyObject* value, aitEnum& result)
{
    if (not value) return false;

    switch (PyObject_IsInstance(value, cas::enum_type)) {
        case 1:
            break;
        case 0:
            PyErr_SetString(PyExc_TypeError, "Return value must be FieldType value");
        default:
            return false;
    };

    PyObject* py_val = PyObject_GetAttrString(value, "value");
    if (not py_val) return false;

    int enum_value = PyLong_AsUnsignedLong(py_val);
    Py_DECREF(py_val);
    if (PyErr_Occurred()) return false;

    switch (enum_value) {
        case DBF_STRING:
            result = aitEnumString;
            break;
        case DBF_ENUM:
            result = aitEnumEnum16;
            break;
        case DBF_CHAR:
            result = aitEnumInt8;
            break;
        case DBF_SHORT:
            result = aitEnumInt16;
            break;
        case DBF_LONG:
            result = aitEnumInt32;
            break;
        case DBF_FLOAT:
            result = aitEnumFloat32;
            break;
        case DBF_DOUBLE:
            result = aitEnumFloat64;
            break;
        default:
            PyErr_SetString(PyExc_TypeError, "Unhandled FieldType value");
            return false;
    }
    return true;
}

bool to_gdd(PyObject* dict, aitEnum type, gdd &result)
{
    if (not dict) return false;

    int app = result.applicationType();

    switch (app) {
        // STS and TIME are also gddAppType_value because status, severity and
        // timestamp are stored in the gdd itself.
        // All STRING types are also gddAppType_value
        case gddAppType_value:
            return read_simple(dict, type, result);
        case gddAppType_enums:
            return read_enums(dict, result);

        case gddAppType_dbr_gr_enum:
            return read_enum(dict, type, result);
        case gddAppType_dbr_gr_char:
            return read_gr(dict, type, result);
        case gddAppType_dbr_gr_short:
            return read_gr(dict, type, result);
        case gddAppType_dbr_gr_long:
            return read_gr(dict, type, result);
        case gddAppType_dbr_gr_float:
            return read_gr(dict, type, result);
        case gddAppType_dbr_gr_double:
            return read_gr(dict, type, result);

        case gddAppType_dbr_ctrl_enum:
            return read_enum(dict, type, result);
        case gddAppType_dbr_ctrl_char:
            return read_ctrl(dict, type, result);
        case gddAppType_dbr_ctrl_short:
            return read_ctrl(dict, type, result);
        case gddAppType_dbr_ctrl_long:
            return read_ctrl(dict, type, result);
        case gddAppType_dbr_ctrl_float:
            return read_ctrl(dict, type, result);
        case gddAppType_dbr_ctrl_double:
            return read_ctrl(dict, type, result);
    }

    char* app_name = gddApplicationTypeTable::app_table.getName(app);
    if (app_name) {
        PyErr_Format(PyExc_RuntimeError, "Unhandled gdd application type: %s", app_name);
    } else {
        PyErr_SetString(PyExc_RuntimeError, "Unhandled gdd application type");
    }
    return false;
}

PyObject* from_gdd(gdd const& value)
{
    int app = value.applicationType();
    if (app != gddAppType_value) {
        char* app_name = gddApplicationTypeTable::app_table.getName(app);
        if (app_name) {
            PyErr_Format(PyExc_RuntimeError, "Unhandled gdd application type: %s", app_name);
        } else {
            PyErr_SetString(PyExc_RuntimeError, "Unhandled gdd application type");
        }
        return nullptr;
    }

    PyObject* val = nullptr;
    switch (value.primitiveType()) {
        case aitEnumString:
            val = write_string(value);
            break;
        case aitEnumEnum16:
            val = write_value<aitEnum16>(value);
            break;
        case aitEnumInt8:
            val = write_value<aitInt8>(value);
            break;
        case aitEnumInt16:
            val = write_value<aitInt16>(value);
            break;
        case aitEnumInt32:
            val = write_value<aitInt32>(value);
            break;
        case aitEnumFloat32:
            val = write_value<aitFloat32>(value);
            break;
        case aitEnumFloat64:
            val = write_value<aitFloat64>(value);
            break;
        default:
            PyErr_SetString(PyExc_RuntimeError, "Unhandled gdd primitive type");
            break;
    }

    return Py_BuildValue("(NN)", val, write_timestamp(value));
}

bool to_event_mask(PyObject* value, casEventMask& mask, caServer const& server)
{
    if (not value) return false;

    PyObject* py_val = PyObject_GetAttrString(value, "value");
    if (not py_val) return false;

    int flag_value = PyLong_AsUnsignedLong(py_val);
    Py_DECREF(py_val);
    if (PyErr_Occurred()) return false;

    if (flag_value & DBE_VALUE) {
        mask |= server.valueEventMask();
    }
    if (flag_value & DBE_ARCHIVE) {
        mask |= server.logEventMask();
    }
    if (flag_value & DBE_ALARM) {
        mask |= server.alarmEventMask();
    }
    if (flag_value & DBE_PROPERTY) {
        mask |= server.propertyEventMask();
    }

    return true;
}

}
