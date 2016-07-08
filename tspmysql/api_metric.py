#!/usr/bin/env python
import tspapi

api = tspapi.API()

metric = api.metric_create(name='API_METRIC',
                           display_name='API Metric',
                           display_name_short='API Metric',
                           description='An API metric',
                           default_aggregate='avg',
                           default_resolution=1000,
                           unit='number',
                           _type='STOCK_PRICE')

print(metric)
