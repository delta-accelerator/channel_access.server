#include "async.hpp"

#include <memory>
#include <Python.h>
#include <structmember.h>
#include <casdef.h>
#include "convert.hpp"


namespace cas {
namespace {

struct AsyncContext {
    PyObject_HEAD
    casCtx const* ctx;
    gdd* prototype;
    aitEnum type;
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



class AsyncWriteProxy;
struct AsyncWrite {
    PyObject_HEAD
    bool held_by_server;
    std::unique_ptr<AsyncWriteProxy> proxy;
};
static_assert(std::is_standard_layout<AsyncWrite>::value, "AsyncWrite has to be standard layout to work with the Python API");

class AsyncWriteProxy : public casAsyncWriteIO {
public:
    AsyncWriteProxy(PyObject* async_write_, casCtx const& ctx)
        : casAsyncWriteIO{ctx}, async_write{async_write_}
    {
        // No GIL, don't use the python API
    }

    PyObject* post(caStatus status)
    {
        AsyncWrite* async = reinterpret_cast<AsyncWrite*>(async_write);

        caStatus result = postIOCompletion(status);
        switch (result) {
            case S_cas_success :
            case S_cas_redundantPost :
                break;
            default:
                PyErr_SetString(PyExc_RuntimeError, "Could not post write IO completion");
                return nullptr;
        }
        Py_RETURN_NONE;
    }

    static PyObject* complete(PyObject* self, PyObject*)
    {
        AsyncWrite* async = reinterpret_cast<AsyncWrite*>(self);
        AsyncWriteProxy* proxy = async->proxy.get();

        return proxy->post(S_casApp_success);
    }

    static PyObject* fail(PyObject* self, PyObject*)
    {
        AsyncWrite* async = reinterpret_cast<AsyncWrite*>(self);
        AsyncWriteProxy* proxy = async->proxy.get();

        return proxy->post(S_casApp_canceledAsyncIO);
    }

private:
    PyObject* async_write;

    virtual void destroy() override
    {
        AsyncWrite* async = reinterpret_cast<AsyncWrite*>(async_write);

        PyGILState_STATE gstate = PyGILState_Ensure();
            // the caServer released its ownership so we have to decrement the python reference count
            if (async->held_by_server) {
                async->held_by_server = false;
                Py_DECREF(async_write);
            }
        PyGILState_Release(gstate);
    }
};


int async_write_init(PyObject* self, PyObject* args, PyObject*)
{
    AsyncWrite* async_write = reinterpret_cast<AsyncWrite*>(self);

    PyObject* context = nullptr;
    if (not PyArg_ParseTuple(args, "O", &context)) return -1;

    auto* context_type = reinterpret_cast<PyObject*>(&async_context_type);
    switch (PyObject_IsInstance(context, context_type)) {
        case 1:
            break;
        case 0:
            PyErr_SetString(PyExc_TypeError, "context argument must be a context object");
        default:
            return -1;
    }

    AsyncContext* async_context = reinterpret_cast<AsyncContext*>(context);

    async_write->held_by_server = false;
    Py_BEGIN_ALLOW_THREADS
        async_write->proxy.reset(new AsyncWriteProxy(self, *async_context->ctx));
    Py_END_ALLOW_THREADS

    return 0;
}

void async_write_dealloc(PyObject* self)
{
    AsyncWrite* async_write = reinterpret_cast<AsyncWrite*>(self);

    async_write->proxy.reset();

    Py_TYPE(self)->tp_free(self);
}

PyObject* async_write_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    PyObject* self = type->tp_alloc(type, 0);
    if (not self) return nullptr;

    return self;
}

PyDoc_STRVAR(write_complete__doc__, R"(complete()
Signal the successful completion of the asynchronous write.
)");
PyDoc_STRVAR(write_fail__doc__, R"(fail()
Signal a failure in completing the asynchronous write.
)");
PyMethodDef async_write_methods[] = {
    {"complete", static_cast<PyCFunction>(AsyncWriteProxy::complete), METH_NOARGS, write_complete__doc__},
    {"fail",     static_cast<PyCFunction>(AsyncWriteProxy::fail),     METH_NOARGS, write_fail__doc__},
    {nullptr}
};

PyDoc_STRVAR(async_write__doc__, R"(AsyncWrite(context)
Asynchronous write completion class.

Return an object of this class from the :meth:`PV.write()` to
signal an asynchronous write. Then call :meth:`complete()` or
:meth:`fail()` to inform the server about the completion status.

Args:
    context: Context object given to the :meth:`PV.write()` method.
)");
PyTypeObject async_write_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "ca_server.cas.AsyncWrite",                /* tp_name */
    sizeof(AsyncWrite),                        /* tp_basicsize */
    0,                                         /* tp_itemsize */
    async_write_dealloc,                       /* tp_dealloc */
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
    async_write__doc__,                        /* tp_doc */
    nullptr,                                   /* tp_traverse */
    nullptr,                                   /* tp_clear */
    nullptr,                                   /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    nullptr,                                   /* tp_iter */
    nullptr,                                   /* tp_iternext */
    async_write_methods,                       /* tp_methods */
    nullptr,                                   /* tp_members */
    nullptr,                                   /* tp_getset */
    nullptr,                                   /* tp_base */
    nullptr,                                   /* tp_dict */
    nullptr,                                   /* tp_descr_get */
    nullptr,                                   /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    async_write_init,                          /* tp_init */
    nullptr,                                   /* tp_alloc */
    async_write_new,                           /* tp_new */
};



class AsyncReadProxy;
struct AsyncRead {
    PyObject_HEAD
    bool held_by_server;
    aitEnum type;
    std::unique_ptr<AsyncReadProxy> proxy;
};
static_assert(std::is_standard_layout<AsyncRead>::value, "AsyncRead has to be standard layout to work with the Python API");


class AsyncReadProxy : public casAsyncReadIO {
public:
    AsyncReadProxy(PyObject* async_read_, casCtx const& ctx, gdd& prototype_)
        : casAsyncReadIO{ctx}, async_read{async_read_}, prototype{&prototype_}
    {
        prototype->reference();
    }

    ~AsyncReadProxy()
    {
        prototype->unreference();
    }

    PyObject* post(caStatus status)
    {
        AsyncRead* async = reinterpret_cast<AsyncRead*>(async_read);

        caStatus result = postIOCompletion(status, *prototype);
        switch (result) {
            case S_cas_success :
            case S_cas_redundantPost :
                break;
            default:
                PyErr_SetString(PyExc_RuntimeError, "Could not post write IO completion");
                return nullptr;
        }
        Py_RETURN_NONE;
    }

    static PyObject* complete(PyObject* self, PyObject* args)
    {
        AsyncRead* async = reinterpret_cast<AsyncRead*>(self);
        AsyncReadProxy* proxy = async->proxy.get();

        PyObject* attributes = nullptr;
        if (not PyArg_ParseTuple(args, "O", &attributes)) {
            proxy->post(S_casApp_canceledAsyncIO);
            return nullptr;
        }

        if (not to_gdd(attributes, async->type, *proxy->prototype)) {
            Py_DECREF(attributes);
            proxy->post(S_casApp_canceledAsyncIO);
            return nullptr;
        }

        Py_DECREF(attributes);
        return proxy->post(S_casApp_success);
    }

    static PyObject* fail(PyObject* self, PyObject*)
    {
        AsyncRead* async = reinterpret_cast<AsyncRead*>(self);
        AsyncReadProxy* proxy = async->proxy.get();

        return proxy->post(S_casApp_canceledAsyncIO);
    }

private:
    PyObject* async_read;
    gdd* prototype;

    virtual void destroy() override
    {
        AsyncRead* async = reinterpret_cast<AsyncRead*>(async_read);

        PyGILState_STATE gstate = PyGILState_Ensure();
            // the caServer released its ownership so we have to decrement the python reference count
            if (async->held_by_server) {
                async->held_by_server = false;
                Py_DECREF(async_read);
            }
        PyGILState_Release(gstate);
    }
};


int async_read_init(PyObject* self, PyObject* args, PyObject*)
{
    AsyncRead* async_read = reinterpret_cast<AsyncRead*>(self);

    PyObject* context = nullptr;
    if (not PyArg_ParseTuple(args, "O", &context)) return -1;

    auto* context_type = reinterpret_cast<PyObject*>(&async_context_type);
    switch (PyObject_IsInstance(context, context_type)) {
        case 1:
            break;
        case 0:
            PyErr_SetString(PyExc_TypeError, "context argument must be a context object");
        default:
            return -1;
    }

    AsyncContext* async_context = reinterpret_cast<AsyncContext*>(context);

    async_read->held_by_server = false;
    async_read->type = async_context->type;
    Py_BEGIN_ALLOW_THREADS
        async_read->proxy.reset(new AsyncReadProxy(self, *async_context->ctx, *async_context->prototype));
    Py_END_ALLOW_THREADS

    return 0;
}

void async_read_dealloc(PyObject* self)
{
    AsyncRead* async_read = reinterpret_cast<AsyncRead*>(self);

    async_read->proxy.reset();

    Py_TYPE(self)->tp_free(self);
}

PyObject* async_read_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    PyObject* self = type->tp_alloc(type, 0);
    if (not self) return nullptr;

    return self;
}


PyDoc_STRVAR(read_complete__doc__, R"(complete(attributes)
Signal the successful completion of the asynchronous read.

Args:
    attributes (dict): An attributes dictionary with the requested values.
)");
PyDoc_STRVAR(read_fail__doc__, R"(fail()
Signal a failure in completing the asynchronous read.
)");
PyMethodDef async_read_methods[] = {
    {"complete", static_cast<PyCFunction>(AsyncReadProxy::complete), METH_VARARGS, read_complete__doc__},
    {"fail",     static_cast<PyCFunction>(AsyncReadProxy::fail),     METH_NOARGS,  read_fail__doc__},
    {nullptr}
};

PyMemberDef async_read_members[] = {
    {nullptr}
};

PyDoc_STRVAR(async_read__doc__, R"(AsyncRead(context)
Asynchronous read completion class.

Return an object of this class from the :meth:`PV.read()` to
signal an asynchronous read. Then call :meth:`complete()` or
:meth:`fail()` to inform the server about the completion status.

Args:
    context: Context object given to the :meth:`PV.read()` method.
)");
PyTypeObject async_read_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "ca_server.cas.AsyncRead",                 /* tp_name */
    sizeof(AsyncRead),                         /* tp_basicsize */
    0,                                         /* tp_itemsize */
    async_read_dealloc,                        /* tp_dealloc */
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
    async_read__doc__,                         /* tp_doc */
    nullptr,                                   /* tp_traverse */
    nullptr,                                   /* tp_clear */
    nullptr,                                   /* tp_richcompare */
    0,                                         /* tp_weaklistoffset */
    nullptr,                                   /* tp_iter */
    nullptr,                                   /* tp_iternext */
    async_read_methods,                        /* tp_methods */
    async_read_members,                        /* tp_members */
    nullptr,                                   /* tp_getset */
    nullptr,                                   /* tp_base */
    nullptr,                                   /* tp_dict */
    nullptr,                                   /* tp_descr_get */
    nullptr,                                   /* tp_descr_set */
    0,                                         /* tp_dictoffset */
    async_read_init,                           /* tp_init */
    nullptr,                                   /* tp_alloc */
    async_read_new,                            /* tp_new */
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


PyObject* create_async_write_type()
{
    if (PyType_Ready(&async_write_type) < 0) return nullptr;

    Py_INCREF(&async_write_type);
    return reinterpret_cast<PyObject*>(&async_write_type);
}

void destroy_async_write_type()
{
    Py_DECREF(&async_write_type);
}


PyObject* create_async_read_type()
{
    if (PyType_Ready(&async_read_type) < 0) return nullptr;

    Py_INCREF(&async_read_type);
    return reinterpret_cast<PyObject*>(&async_read_type);
}

void destroy_async_read_type()
{
    Py_DECREF(&async_read_type);
}


PyObject* create_async_context(casCtx const& ctx, gdd* prototype, aitEnum type)
{
    AsyncContext* context = PyObject_New(AsyncContext, &async_context_type);

    context->ctx = &ctx;
    context->prototype = prototype;
    context->type = type;

    return reinterpret_cast<PyObject*>(context);
}

bool give_async_read_to_server(PyObject* obj)
{
    auto* read_type = reinterpret_cast<PyObject*>(&async_read_type);
    if (PyObject_IsInstance(obj, read_type) != 1) return false;

    AsyncRead* async = reinterpret_cast<AsyncRead*>(obj);
    async->held_by_server = true;
    Py_INCREF(obj); // caServer now holds a reference

    return true;
}

bool give_async_write_to_server(PyObject* obj)
{
    auto* write_type = reinterpret_cast<PyObject*>(&async_write_type);
    if (PyObject_IsInstance(obj, write_type) != 1) return false;

    AsyncWrite* async = reinterpret_cast<AsyncWrite*>(obj);
    async->held_by_server = true;
    Py_INCREF(obj); // caServer now holds a reference

    return true;
}

}
