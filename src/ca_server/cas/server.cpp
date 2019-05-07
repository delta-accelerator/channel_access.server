#include "server.hpp"

#include <memory>
#include <Python.h>
#include <structmember.h>
#include <casdef.h>

#include "cas.hpp"
#include "convert.hpp"

namespace cas {
namespace {

class ServerProxy;
struct Server {
    PyObject_HEAD
    std::unique_ptr<ServerProxy> proxy;
};
static_assert(std::is_standard_layout<Server>::value, "Server has to be standard layout to work with the Python API");


class ServerProxy : public caServer {
public:
    ServerProxy(PyObject* server)
        : server{server}
    {}

    virtual pvExistReturn pvExistTest(casCtx const& ctx,
        caNetAddr const& clientAddress, char const* pPVAliasName) override
    {
        auto address = static_cast<sockaddr_in>(clientAddress);
        unsigned long host = ntohl(address.sin_addr.s_addr);
        unsigned short port = ntohs(address.sin_port);

        pvExistReturn ret = pverDoesNotExistHere;
        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* fn = PyObject_GetAttrString(server, "pvExistTest");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, "(kH)y", host, port, pPVAliasName);
                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }
                Py_DECREF(fn);

                if (result) {
                    to_exist_return(result, ret);
                    Py_DECREF(result);
                }
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(server);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
        return ret;
    }

    static PyObject* pvExistTest(PyObject* self, PyObject* args)
    {
        Server* server = reinterpret_cast<Server*>(self);

        unsigned long host;
        unsigned short port;
        char const* pv_name;
        if (not PyArg_ParseTuple(args, "(kH)y:pvExistTest", &host, &port, &pv_name)) return nullptr;

        return PyObject_GetAttrString(cas::enum_exists, "NOT_EXISTS_HERE");
    }

    virtual pvAttachReturn pvAttach(casCtx const& ctx,
        char const* pPVAliasName) override
    {
        pvAttachReturn ret = S_casApp_pvNotFound;
        PyGILState_STATE gstate = PyGILState_Ensure();
            PyObject* fn = PyObject_GetAttrString(server, "pvAttach");
            if (fn) {
                PyObject* result = PyObject_CallFunction(fn, "y", pPVAliasName);
                if (PyErr_Occurred()) {
                    PyErr_WriteUnraisable(fn);
                    PyErr_Clear();
                }
                Py_DECREF(fn);

                if (result) {
                    to_attach_return(result, ret);
                    Py_DECREF(result);
                }
            }

            if (PyErr_Occurred()) {
                PyErr_WriteUnraisable(server);
                PyErr_Clear();
            }
        PyGILState_Release(gstate);
        return ret;
    }

    static PyObject* pvAttach(PyObject* self, PyObject* args)
    {
        Server* server = reinterpret_cast<Server*>(self);

        char const* pv_name;
        if (not PyArg_ParseTuple(args, "y:pvAttach", &pv_name)) return nullptr;

        return PyObject_GetAttrString(cas::enum_attach, "NOT_FOUND");
    }

    //virtual void show(unsigned level) const override;

private:
    PyObject* server;
};



int server_init(PyObject* self, PyObject* args, PyObject* kwds)
{
    Server* server = reinterpret_cast<Server*>(self);
    return 0;
}

void server_dealloc(PyObject* self)
{
    Server* server = reinterpret_cast<Server*>(self);
    server->proxy.reset();

    Py_TYPE(self)->tp_free(self);
}

PyObject* server_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    PyObject* self = type->tp_alloc(type, 0);
    if (not self) {
        PyErr_SetString(PyExc_RuntimeError, "Could not allocate new Server");
        return nullptr;
    }

    Server* server = reinterpret_cast<Server*>(self);
    try {
        server->proxy.reset(new ServerProxy(self));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Could not create ServerProxy");
        return nullptr;
    }
    return self;
}

PyDoc_STRVAR(pvExistTest__doc__, R"(pvExistTest(address, name)

Test if PV exists
)");
PyDoc_STRVAR(pvAttach__doc__, R"(pvAttach(name)

Create PV handler.
)");

PyMethodDef server_methods[] = {
    {"pvExistTest", static_cast<PyCFunction>(ServerProxy::pvExistTest), METH_VARARGS, pvExistTest__doc__},
    {"pvAttach",    static_cast<PyCFunction>(ServerProxy::pvAttach),    METH_VARARGS, pvAttach__doc__},
    {nullptr}
};

PyMemberDef server_members[] = {
    {nullptr}
};

PyDoc_STRVAR(server__doc__, R"(
Server class.
)");
PyTypeObject server_type = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    "ca_server.cas.Server",                    /* tp_name */
    sizeof(Server),                            /* tp_basicsize */
    0,                                         /* tp_itemsize */
    server_dealloc,                            /* tp_dealloc */
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
    server__doc__,                             /* tp_doc */
    nullptr,                                   /* tp_traverse */
    nullptr,                                   /* tp_clear */
    nullptr,                                   /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    nullptr,                                   /* tp_iter */
    nullptr,                                   /* tp_iternext */
    server_methods,                            /* tp_methods */
    server_members,                            /* tp_members */
    nullptr,                                   /* tp_getset */
    nullptr,                                   /* tp_base */
    nullptr,                                   /* tp_dict */
    nullptr,                                   /* tp_descr_get */
    nullptr,                                   /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    server_init,                               /* tp_init */
    nullptr,                                   /* tp_alloc */
    server_new,                                /* tp_new */
};

}

PyObject* create_server_type()
{
    if (PyType_Ready(&server_type) < 0) return nullptr;

    Py_INCREF(&server_type);
    return reinterpret_cast<PyObject*>(&server_type);
}

void destroy_server_type()
{
    Py_DECREF(&server_type);
}

}
