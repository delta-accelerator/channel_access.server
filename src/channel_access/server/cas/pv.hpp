#ifndef INCLUDE_GUARD_35A32778_12EC_461B_9182_2CD507FA46A3
#define INCLUDE_GUARD_35A32778_12EC_461B_9182_2CD507FA46A3

#include <Python.h>

class casPV;

namespace cas {

/** Create the server type.
 * Returns new reference.
 */
PyObject* create_pv_type();

/** Destroy the server type.
 */
void destroy_pv_type();

/**
 * Return pointer to casPV object from Python Pv object.
 *
 * If this is the first time the Pv is given to the server its reference count is incremented
 * because the server owns the Pv object.
 */
casPV* give_to_server(PyObject* obj);

}

#endif
