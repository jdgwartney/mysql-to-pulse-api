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
import pymysql
import os
import petl
import pymysql

# Fetch our database access configuration from environment variables
host = os.environ['DB_HOST']
user = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
db = os.environ['DB_DATABASE']

# Connect to the database using the PyMSQL package
connection = pymysql.connect(host=host,
                             user=user,
                             password=password,
                             db=db)

# Extract the data using both PyMSQL and PETL
table = petl.fromdb(connection, 'SELECT dt, total, duration FROM ol_transactions')
print(table)

connection.close()
