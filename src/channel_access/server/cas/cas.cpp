#include "cas.hpp"

#include <Python.h>

#include <casdef.h>
#include <fdManager.h>

#include "server.hpp"
#include "pv.hpp"

namespace cas {

PyObject* enum_type = nullptr;
PyObject* enum_status = nullptr;
PyObject* enum_severity = nullptr;

PyObject* enum_exists = nullptr;
PyObject* enum_attach = nullptr;

namespace {

PyDoc_STRVAR(process__doc__, R"(process(timeout)

Process server io for at most ``timeout`` seconds.
)");
PyObject* process(PyObject* module, PyObject* arg)
{
    double timeout = PyFloat_AsDouble(arg);
    if (PyErr_Occurred()) return nullptr;

    Py_BEGIN_ALLOW_THREADS
        fileDescriptorManager.process(timeout);
    Py_END_ALLOW_THREADS

    Py_RETURN_NONE;
}

PyMethodDef methods[] = {
    {"process", process, METH_O, process__doc__},
    {nullptr}   /* Sentinel */
};

PyDoc_STRVAR(cas__doc__, R"(
Low level wrapper module over the cas interface.
)");
PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "channel_access.server.cas", /* name of module */
    cas__doc__,                  /* module documentation, may be NULL */
    -1,                          /* size of per-interpreter state of the module,
                                    or -1 if the module keeps state in global variables. */
    methods,                     /* methods */
};

PyDoc_STRVAR(exists__doc__, R"(
Return value for the :meth:`Server.pvExistTest()` method.
)");
PyObject* add_exists(PyObject* module, PyObject* enum_class)
{
    PyObject* exists = PyObject_CallFunction(enum_class, "s[(si)(si)]", "ExistsResponse",
        "EXISTS_HERE", pverExistsHere,
        "NOT_EXISTS_HERE", pverDoesNotExistHere);
    if (not exists) return nullptr;

    PyObject* doc = PyUnicode_FromString(exists__doc__);
    if (doc) {
        PyObject_SetAttrString(exists, "__doc__", doc);
        Py_DECREF(doc);
    }

    Py_INCREF(exists);
    if (PyModule_AddObject(module, "ExistsResponse", exists) != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Could not add ExistsResponse enum");
        Py_DECREF(exists);
        return nullptr;
    }

    return exists;
}

PyDoc_STRVAR(attach__doc__, R"(
Return value for the :meth:`Server.pvAttach()` method.
)");
PyObject* add_attach(PyObject* module, PyObject* enum_class)
{
    PyObject* attach = PyObject_CallFunction(enum_class, "s[(si)(si)]", "AttachResponse",
        "NO_MEMORY", S_casApp_noMemory,
        "NOT_FOUND", S_casApp_pvNotFound);
    if (not attach) return nullptr;

    PyObject* doc = PyUnicode_FromString(attach__doc__);
    if (doc) {
        PyObject_SetAttrString(attach, "__doc__", doc);
        Py_DECREF(doc);
    }

    Py_INCREF(attach);
    if (PyModule_AddObject(module, "AttachResponse", attach) != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Could not add AttachResponse enum");
        Py_DECREF(attach);
        return nullptr;
    }

    return attach;
}

} // namespace
} // namespace cas

extern "C" {

PyMODINIT_FUNC PyInit_cas(void)
{
    int result = -1;
    PyObject* module = nullptr, *server_type = nullptr, *pv_type = nullptr;
    PyObject* ca_module = nullptr;
    PyObject* enum_module = nullptr, *enum_class = nullptr;

    module = PyModule_Create(&cas::module);
    if (not module) goto error;


    ca_module = PyImport_ImportModule("channel_access.common");
    if (not ca_module) goto error;

    cas::enum_type = PyObject_GetAttrString(ca_module, "Type");
    if (not cas::enum_type) goto error;

    cas::enum_status = PyObject_GetAttrString(ca_module, "Status");
    if (not cas::enum_status) goto error;

    cas::enum_severity = PyObject_GetAttrString(ca_module, "Severity");
    if (not cas::enum_severity) goto error;


    enum_module = PyImport_ImportModule("enum");
    if (not enum_module) goto error;
    enum_class = PyObject_GetAttrString(enum_module, "Enum");
    if (not enum_class) goto error;

    cas::enum_exists = cas::add_exists(module, enum_class);
    if (not cas::enum_exists) goto error;

    cas::enum_attach = cas::add_attach(module, enum_class);
    if (not cas::enum_attach) goto error;


    server_type = cas::create_server_type();
    if (not server_type) goto error;

    result = PyModule_AddObject(module, "Server", server_type);
    server_type = nullptr;
    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Could not add Server class");
        goto error;
    }

    pv_type = cas::create_pv_type();
    if (not pv_type) goto error;

    result = PyModule_AddObject(module, "PV", pv_type);
    pv_type = nullptr;
    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Could not add PV class");
        goto error;
    }

    Py_DECREF(enum_class);
    Py_DECREF(enum_module);
    Py_DECREF(ca_module);
    return module;

error:
    Py_XDECREF(pv_type);
    Py_XDECREF(server_type);
    Py_XDECREF(cas::enum_attach);
    Py_XDECREF(cas::enum_exists);
    Py_XDECREF(cas::enum_severity);
    Py_XDECREF(cas::enum_status);
    Py_XDECREF(cas::enum_type);
    Py_XDECREF(enum_class);
    Py_XDECREF(enum_module);
    Py_XDECREF(ca_module);
    Py_XDECREF(module);
    return nullptr;
}

}
