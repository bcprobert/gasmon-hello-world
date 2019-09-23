GasMon
===========

A basic proof of concept event reader

Running the application
-----------------------

To run the application, first ensure that you have installed Poetry_ on your system. Install
dependencies using the command ``poetry install``.

To run the application, set configuration values in ``config.yaml`` and then use the command ``poetry run start``. 
This will run the GasMon analyser for the configured period of time.

Running the tests
-----------------

To run the tests, use the command ``poetry run pytest tests``.

.. _Poetry: https://github.com/sdispater/poetry