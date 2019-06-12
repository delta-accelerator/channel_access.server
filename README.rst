Channel Access server library
=============================

This library contains a low-level binding to the cas library in EPICS base
and a thread-safe high level interface to create channel access servers.

Installation
------------
Before installing the library, the environment variables ``EPICS_BASE``
and ``EPICS_HOST_ARCH`` must be set.

Then the library can be installed with pip::

    pip install channel_access.server

Example
-------
This example shows a simple server with a PV counting up:

.. code:: python

    import time
    import channel_access.common as ca
    import channel_access.server as cas

    with cas.Server() as server:
        pv = server.createPV('CAS:Test', ca.Type.LONG)
        while True:
            pv.value += 1
            time.sleep(1.0)

Documentation
-------------
The documentation is available `online`_ or it can be
generated from the source code with *sphinx*::

    cd /path/to/repository
    pip install -e .
    python setup.py build_sphinx

Then open ``build/sphinx/html/index.html``.

.. _online: https://delta-accelerator.github.io/channel_access.server

Get the source
--------------
The source code is available in a `Github repository`_::

    git clone https://github.com/delta-accelerator/channel_access.server

.. _Github repository: https://github.com/delta-accelerator/channel_access.server

Tests
-----
Tests are run with *pytest*::

    cd /path/to/repository
    pytest -v

To run the tests for all supported version use *tox*::

    cd /path/to/repository
    tox
