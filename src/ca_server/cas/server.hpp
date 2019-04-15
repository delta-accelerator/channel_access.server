#ifndef INCLUDE_GUARD_0D810E02_8C35_4A01_B622_DFC6082FB808
#define INCLUDE_GUARD_0D810E02_8C35_4A01_B622_DFC6082FB808

#include <Python.h>

namespace cas {

/** Create the server type.
 * Returns new reference.
 */
PyObject* create_server_type();

/** Destroy the server type.
 */
void destroy_server_type();

}

#endif
