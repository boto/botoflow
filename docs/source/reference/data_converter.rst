==============
Data Converter
==============

Data converters are used in botoflow to serialize/deserialize Python objects and exceptions between botoflow and Amazon SWF.

.. note::

   The naming ``data_converters`` and not ``serde`` or ``encoder/decoder`` was chosen to match closer with the naming in official Flow Java library, but behaves much like :py:func:`json.dumps` and :py:func:`json.loads` in one.
   


Abstract Data Converter
-----------------------

.. automodule:: botoflow.data_converter.abstract_data_converter
   :members:

JSON Data Converter
-------------------

.. automodule:: botoflow.data_converter.json_data_converter
   :members:

Pickle Data Converter
---------------------

.. automodule:: botoflow.data_converter.pickle_data_converter
   :members:
