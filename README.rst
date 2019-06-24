Channel Access server library
=============================

This library contains a low-level binding to the cas library in EPICS base
and a thread-safe high level interface to create channel access servers.

For the client implementation see `channel_access.client`_.

.. _channel_access.client: https://pypi.org/project/channel_access.client

Installation
------------
Before installing the library, the environment variables ``EPICS_BASE``
and ``EPICS_HOST_ARCH`` must be set.

Then the library can be installed with pip::

    pip install channel_access.server

If *numpy* can be imported at install time, numpy support is automatically
activated. This can be explicitly controlled with the environment variable
``CA_WITH_NUMPY``::

    CA_WITH_NUMPY=0 pip install channel_access.server
    CA_WITH_NUMPY=1 pip install channel_access.server

If the package is compiled with numpy support, numpy arrays are used
by default. If numpy arrays should not be used, the parameter ``use_numpy``
can be set to ``False``.

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
