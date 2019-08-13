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


/** Create an asnyc context object.
 */
PyObject* create_async_context(casCtx const& ctx);

}

#endif
