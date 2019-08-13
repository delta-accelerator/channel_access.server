#include "async.hpp"

#include <memory>
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


PyObject* create_async_context(casCtx const& ctx)
{
    AsyncContext* context = PyObject_New(AsyncContext, &async_context_type);

    context->ctx = &ctx;

    return reinterpret_cast<PyObject*>(context);
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
