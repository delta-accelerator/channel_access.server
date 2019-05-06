#include "pv.hpp"

#include <memory>
#include <Python.h>
#include <structmember.h>
#include <casdef.h>
#include <gddApps.h>

#include "cas.hpp"
#include "convert.hpp"

namespace cas {
namespace {

class PvProxy;
struct Pv {
    PyObject_HEAD
    char* name;
    bool held_by_server;
    std::unique_ptr<PvProxy> proxy;
};
static_assert(std::is_standard_layout<Pv>::value, "Pv has to be standard layout to work with the Python API");


class PvProxy : public casPV {
public:
    PvProxy(PyObject* pv)
        : pv{pv}
    {}

    char const* getName() const override
    {
        return reinterpret_cast<Pv*>(pv)->name;
    }

    virtual void destroy() override
    {
        Pv* pv_struct = reinterpret_cast<Pv*>(pv);

        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* fn = PyObject_GetAttrString(pv, "destroy");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, nullptr);
                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }

                Py_DECREF(fn);
                Py_XDECREF(result);
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(pv);
                PyErr_Clear();
            }

            // the caServer released its ownership so we have to decrement the python reference count
            if (pv_struct->held_by_server) {
                pv_struct->held_by_server = false;
                Py_DECREF(pv);
            }
        PyGILState_Release(gstate);
    }

    static PyObject* destroy(PyObject* self, PyObject*)
    {
        Py_RETURN_NONE;
    }

    virtual aitEnum bestExternalType() const override
    {
        aitEnum ret = aitEnumString;
        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* fn = PyObject_GetAttrString(pv, "type");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, nullptr);
                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }
                Py_DECREF(fn);

                if (result) {
                    to_ait_enum(result, ret);
                    Py_DECREF(result);
                }
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(pv);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
        return ret;
    }

    static PyObject* type(PyObject* self, PyObject*)
    {
        return PyObject_GetAttrString(cas::enum_type, "STRING");
    }

    virtual unsigned maxDimension() const override
    {
        unsigned ret = 0;
        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* fn = PyObject_GetAttrString(pv, "count");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, nullptr);
                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }

                if (PyLong_Check(result)) {
                    long count = PyLong_AsLong(result);
                    ret = count > 1;
                }
                Py_XDECREF(result);
                Py_DECREF(fn);
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(pv);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
        return ret;
    }

    virtual aitIndex maxBound(unsigned dimension) const override
    {
        aitIndex ret = 0;
        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* fn = PyObject_GetAttrString(pv, "count");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, nullptr);
                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }

                if (result) {
                    aitIndex bound = PyLong_AsLong(result);
                    if (not PyErr_Occurred()) {
                        ret = bound;
                    }

                    Py_DECREF(result);
                }
                Py_DECREF(fn);
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(pv);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
        return ret;
    }

    static PyObject* count(PyObject* self, PyObject*)
    {
        return PyLong_FromLong(1);
    }

    virtual caStatus read(casCtx const& ctx, gdd& prototype) override
    {
        aitEnum type = aitEnumInvalid;
        caStatus ret = S_casApp_noSupport;
        PyGILState_STATE gstate = PyGILState_Ensure();
            {
                PyObject* fn = PyObject_GetAttrString(pv, "type");
                if (fn) {
                    PyObject* result = PyObject_CallFunction(fn, nullptr);
                    if (PyErr_Occurred()) {
                        PyErr_WriteUnraisable(fn);
                        PyErr_Clear();
                    }
                    Py_DECREF(fn);

                    if (result) {
                        to_ait_enum(result, type);
                        Py_DECREF(result);
                    }
                }
            }

            if (type != aitEnumInvalid) {
                PyObject* fn = PyObject_GetAttrString(pv, "read");
                if (fn) {
                    PyObject* result = PyObject_CallFunction(fn, nullptr);
                    if (PyErr_Occurred()) {
                        PyErr_WriteUnraisable(fn);
                        PyErr_Clear();
                    }
                    Py_DECREF(fn);

                    if (result and result != Py_None) {
                        if (to_gdd(result, type, prototype)) {
                            ret = S_casApp_success;
                        }
                        Py_DECREF(result);
                    }
                }
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(pv);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
        return ret;
    }

    static PyObject* read(PyObject* self, PyObject*)
    {
        Py_RETURN_NONE;
    }

    virtual caStatus write(casCtx const& ctx, gdd const& value) override
    {

        caStatus ret = S_casApp_noSupport;
        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* args = from_gdd(value);
            if (args) {
                PyObject* fn = PyObject_GetAttrString(pv, "write");
                if (fn) {
                    PyObject* result = PyObject_CallObject(fn, args);
                    if (PyErr_Occurred()) {
                        PyErr_WriteUnraisable(fn);
                        PyErr_Clear();
                    }
                    Py_DECREF(fn);

                    if (result) {
                        if (PyObject_IsTrue(result)) {
                            ret = S_casApp_success;
                        }
                        Py_DECREF(result);
                    }
                }

                Py_DECREF(args);
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(pv);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
        return ret;
    }

    static PyObject* write(PyObject* self, PyObject* args)
    {
        Py_RETURN_NONE;
    }

    static PyObject* postEvent(PyObject* self, PyObject* args)
    {
        PvProxy* proxy = reinterpret_cast<Pv*>(self)->proxy.get();
        PyObject* py_events = nullptr, *py_values = nullptr;

        if (not PyArg_ParseTuple(args, "OO", &py_events, &py_values)) return nullptr;

        aitEnum type = aitEnumInvalid;
        {
            PyObject* fn = PyObject_GetAttrString(proxy->pv, "type");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, nullptr);
                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }
                Py_DECREF(fn);

                if (result) {
                    to_ait_enum(result, type);
                    Py_DECREF(result);
                }
            }
        }

        if (type != aitEnumInvalid) {
            casEventMask mask;
            caServer const* server = static_cast<casPV*>(proxy)->getCAS();
            if (server and to_event_mask(py_events, mask, *server)) {
                auto* values = new gdd{gddAppType_value};

                try {
                    if (to_gdd(py_values, type, *values)) {
                        static_cast<casPV*>(proxy)->postEvent(mask, *values);
                    }
                } catch (...) {
                    values->unreference();
                    throw;
                }
                values->unreference();
            }
        }

        if (PyErr_Occurred()) {
            PyErr_WriteUnraisable(proxy->pv);
            PyErr_Clear();
        }

        Py_RETURN_NONE;
    }

    virtual caStatus interestRegister() override
    {
        caStatus ret = S_casApp_noSupport;
        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* fn = PyObject_GetAttrString(pv, "interestRegister");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, nullptr);
                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }
                Py_DECREF(fn);

                if (result) {
                    if (PyObject_IsTrue(result)) {
                        ret = S_casApp_success;
                    }
                    Py_DECREF(result);
                }
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(pv);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
        return ret;
    }

    static PyObject* interestRegister(PyObject* self, PyObject*)
    {
        Py_RETURN_NONE;
    }

    virtual void interestDelete() override
    {
        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* fn = PyObject_GetAttrString(pv, "interestDelete");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, nullptr);
                Py_XDECREF(result);

                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }
                Py_DECREF(fn);
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(pv);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
    }

    static PyObject* interestDelete(PyObject* self, PyObject*)
    {
        Py_RETURN_NONE;
    }

private:
    PyObject* pv;
};


int pv_init(PyObject* self, PyObject* args, PyObject*)
{
    Pv* pv = reinterpret_cast<Pv*>(self);

    char const *c_name;
    if (not PyArg_ParseTuple(args, "y", &c_name)) return -1;

    pv->name = strdup(c_name);
    pv->held_by_server = false;
    return 0;
}

void pv_dealloc(PyObject* self)
{
    Pv* pv = reinterpret_cast<Pv*>(self);

    free(pv->name);
    pv->proxy.reset();

    Py_TYPE(self)->tp_free(self);
}

PyObject* pv_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    PyObject* self = type->tp_alloc(type, 0);
    if (not self) return nullptr;

    Pv* pv = reinterpret_cast<Pv*>(self);
    pv->proxy.reset(new PvProxy(self));
    return self;
}

// we can't put these inside the PvProxy class
PyDoc_STRVAR(destroy__doc__, R"(destroy()

blub
)");
PyDoc_STRVAR(type__doc__, R"(type()

blub
)");
PyDoc_STRVAR(count__doc__, R"(count()

blub
)");
PyDoc_STRVAR(read__doc__, R"(read()

blub
)");
PyDoc_STRVAR(write__doc__, R"(write()

blub
)");
PyDoc_STRVAR(postEvent__doc__, R"(postEvent()

blub
)");
PyDoc_STRVAR(interestRegister__doc__, R"(interestRegister()

blub
)");
PyDoc_STRVAR(interestDelete__doc__, R"(interestDelete()

blub
)");

PyMethodDef pv_methods[] = {
    {"destroy",          static_cast<PyCFunction>(PvProxy::destroy),          METH_NOARGS,  destroy__doc__},
    {"type",             static_cast<PyCFunction>(PvProxy::type),             METH_NOARGS,  type__doc__},
    {"count",            static_cast<PyCFunction>(PvProxy::count),            METH_NOARGS,  count__doc__},
    {"read",             static_cast<PyCFunction>(PvProxy::read),             METH_NOARGS,  read__doc__},
    {"write",            static_cast<PyCFunction>(PvProxy::write),            METH_VARARGS, write__doc__},
    {"postEvent",        static_cast<PyCFunction>(PvProxy::postEvent),        METH_VARARGS, postEvent__doc__},
    {"interestRegister", static_cast<PyCFunction>(PvProxy::interestRegister), METH_NOARGS,  interestRegister__doc__},
    {"interestDelete",   static_cast<PyCFunction>(PvProxy::interestDelete),   METH_NOARGS,  interestDelete__doc__},
    {nullptr}
};

PyDoc_STRVAR(name__doc__, R"(getName()

blub
)");
PyMemberDef pv_members[] = {
    {"name",  T_STRING, offsetof(Pv, name), 1, name__doc__}, // READONLY
    {nullptr}
};

PyDoc_STRVAR(pv__doc__, R"(
server pv class

blub
)");
PyTypeObject pv_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "ca_server.cas.PV",                        /* tp_name */
    sizeof(Pv),                                /* tp_basicsize */
    0,                                         /* tp_itemsize */
    pv_dealloc,                                /* tp_dealloc */
    nullptr,                                   /* tp_print */
    nullptr,                                   /* tp_getattr */
    nullptr,                                   /* tp_setattr */
    nullptr,                                   /* tp_as_async */
    nullptr,                                   /* tp_repr */
    nullptr,                                   /* tp_as_number */
    nullptr,                                   /* tp_as_sequence */
    nullptr,                                   /* tp_as_mapping */
    nullptr,                                   /* tp_hash */
    nullptr,                                   /* tp_call */
    nullptr,                                   /* tp_str */
    nullptr,                                   /* tp_getattro */
    nullptr,                                   /* tp_setattro */
    nullptr,                                   /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,  /* tp_flags */
    pv__doc__,                                 /* tp_doc */
    nullptr,                                   /* tp_traverse */
    nullptr,                                   /* tp_clear */
    nullptr,                                   /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    nullptr,                                   /* tp_iter */
    nullptr,                                   /* tp_iternext */
    pv_methods,                                /* tp_methods */
    pv_members,                                /* tp_members */
    nullptr,                                   /* tp_getset */
    nullptr,                                   /* tp_base */
    nullptr,                                   /* tp_dict */
    nullptr,                                   /* tp_descr_get */
    nullptr,                                   /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    pv_init,                                   /* tp_init */
    nullptr,                                   /* tp_alloc */
    pv_new,                                    /* tp_new */
};

}

PyObject* create_pv_type()
{
    if (PyType_Ready(&pv_type) < 0) return nullptr;

    Py_INCREF(&pv_type);
    return reinterpret_cast<PyObject*>(&pv_type);
}

void destroy_pv_type()
{
    Py_DECREF(&pv_type);
}

casPV* give_to_server(PyObject* obj) {
    switch (PyObject_IsInstance(obj, reinterpret_cast<PyObject*>(&pv_type))) {
        case 1:
            break;
        case 0:
            PyErr_SetString(PyExc_TypeError, "Return value must be a PV instance");
        default:
            return nullptr;
    }

    Pv* pv = reinterpret_cast<Pv*>(obj);
    if (not pv->held_by_server) {
        Py_INCREF(obj);
        pv->held_by_server = true;
    }
    return pv->proxy.get();
}


}
