#include "async.hpp"

#include <Python.h>
#include <structmember.h>
#include <casdef.h>


namespace cas {
namespace {

struct AsyncContext {
    PyObject_HEAD
    casCtx const* ctx;
};
static_assert(std::is_standard_layout<AsyncContext>::value, "AsyncContext has to be standard layout to work with the Python API");


void async_context_dealloc(PyObject* self)
{
    AsyncContext* async_context = reinterpret_cast<AsyncContext*>(self);

    Py_TYPE(self)->tp_free(self);
}


PyDoc_STRVAR(async_context__doc__, R"(
Asynchronous context object.
)");
PyTypeObject async_context_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "ca_server.cas.AsyncContext",              /* tp_name */
    sizeof(AsyncContext),                      /* tp_basicsize */
    0,                                         /* tp_itemsize */
    async_context_dealloc,                     /* tp_dealloc */
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
    Py_TPFLAGS_DEFAULT,                        /* tp_flags */
    async_context__doc__,                      /* tp_doc */
    nullptr,                                   /* tp_traverse */
    nullptr,                                   /* tp_clear */
    nullptr,                                   /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    nullptr,                                   /* tp_iter */
    nullptr,                                   /* tp_iternext */
    nullptr,                                   /* tp_methods */
    nullptr,                                   /* tp_members */
    nullptr,                                   /* tp_getset */
    nullptr,                                   /* tp_base */
    nullptr,                                   /* tp_dict */
    nullptr,                                   /* tp_descr_get */
    nullptr,                                   /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    nullptr,                                   /* tp_init */
    nullptr,                                   /* tp_alloc */
    nullptr,                                   /* tp_new */
};

}


PyObject* create_async_context_type()
{
    if (PyType_Ready(&async_context_type) < 0) return nullptr;

    Py_INCREF(&async_context_type);
    return reinterpret_cast<PyObject*>(&async_context_type);
}

void destroy_async_context_type()
{
    Py_DECREF(&async_context_type);
}


PyObject* create_async_context(casCtx const& ctx)
{
    AsyncContext* context = PyObject_New(AsyncContext, &async_context_type);

    context->ctx = &ctx;

    return reinterpret_cast<PyObject*>(context);
}

}
