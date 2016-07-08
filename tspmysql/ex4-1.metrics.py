#!/usr/bin/env python
#
# Copyright 2016 BMC Software, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import tspapi

api = tspapi.API()

# Define the variables for our metric definition
name = 'MY_FIRST_METRIC'
display_name = 'My First Metric'
display_name_short = 'My 1st Metric'
description = 'First metric created in my account'
default_aggregate = tspapi.aggregates.MAX
default_resolution = 5000
unit = tspapi.units.NUMBER

# Call the metric API to create
metric = api.metric_create(name=name, display_name=display_name,
                           display_name_short=display_name_short,
                           description=description,
                           default_aggregate=default_aggregate,
                           default_resolution=default_resolution,
                           _type='DEVICE',
                           unit=unit)

# Print out the returned metric
print(metric.name)
