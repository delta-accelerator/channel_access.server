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

.. literalinclude:: ../example.py
    :language: python

API Reference
-------------

.. toctree::

    server.rst
    server.cas.rst
