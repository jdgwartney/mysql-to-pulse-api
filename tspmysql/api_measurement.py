#!/usr/bin/env python
import tspapi

api = tspapi.API()

api.measurement_create(metric='CPU',
                       value=0.8,
                       source='my-server')

