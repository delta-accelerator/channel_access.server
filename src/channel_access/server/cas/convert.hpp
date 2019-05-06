#ifndef INCLUDE_GUARD_120C2201_BD9B_465C_B2BD_70FE4AA4A6BD
#define INCLUDE_GUARD_120C2201_BD9B_465C_B2BD_70FE4AA4A6BD

#include <Python.h>

#include <casdef.h>

namespace cas {

/** Convert ExistsResponse enum value to pvExistsReturn value
 */
bool to_exist_return(PyObject* value, pvExistReturn& result);

/** Convert AttachResponse to pvAttachReturn value
 */
bool to_attach_return(PyObject* value, pvAttachReturn& result);

/** convert FieldType enum value to aitEnum value
 */
bool to_ait_enum(PyObject* value, aitEnum& result);

/** convert python value to gdd value
 * use type as the value type.
 * convert any sequence to an array.
 */
bool to_gdd(PyObject* dict, aitEnum type, gdd &result);

/** convert a gdd value to a python value
 * If compiled without numpy support, numpy is always false.
 * For array values:
 *   if numpy is true create a numpy array otherwise create a tuple.
 */
PyObject* from_gdd(gdd const& value, bool numpy = false);

/** convert python Trigger value to caEvent mask value
 */
bool to_event_mask(PyObject* value, casEventMask& mask, caServer const& server);

}

#endif
