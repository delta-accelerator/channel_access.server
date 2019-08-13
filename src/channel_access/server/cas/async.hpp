#ifndef INCLUDE_GUARD_89374487_8D33_448E_B992_AA6B5C8548A5
#define INCLUDE_GUARD_89374487_8D33_448E_B992_AA6B5C8548A5

#include <Python.h>
#include <casdef.h>

namespace cas {

/** Create the AsyncContext type.
 * Returns new reference.
 */
PyObject* create_async_context_type();

/** Destroy the AsyncContext type.
 */
void destroy_async_context_type();


/** Create the AsyncWrite type.
 * Returns new reference.
 */
PyObject* create_async_write_type();

/** Destroy the AsyncWrite type.
 */
void destroy_async_write_type();


/** Create an asnyc context object.
 * Returns new reference.
 */
PyObject* create_async_context(casCtx const& ctx);

/** Try to give an async write handler object to the server.
 *
 * Returns:
 *  ``True`` if the object is an async write object and is given to the server.
 */
bool give_async_write_to_server(PyObject* obj);

}

#endif
