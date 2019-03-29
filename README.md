torpcserver
===============

This library is an implementation of both the JSON-RPC specification (server-side) for the Tornado web framework. It supports the basic features of both, as well as the MultiCall / Batch support for both specifications. The JSON-RPC handler supports both the original specification, as well as the new (proposed) 0.0.1 spec, which includes batch submission, keyword arguments, etc.