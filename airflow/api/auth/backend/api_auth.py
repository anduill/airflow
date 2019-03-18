# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from functools import wraps
from flask import request
from flask import Response
from airflow import configuration

client_auth = None
config = None

def init_app(app):
    global config
    config = configuration.conf


def requires_authentication(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        _config = config
        api_token = config.get("api", "authorization_token")
        header = str(request.headers.get("Authorization"))
        if not api_token:
            return Response("Endpoint Disabled.  Please contact your system administrator to enable this endpoint.", 403)
        if not header or header == 'None':
            return Response("Missing Authorization header required for this endpoint.  Make sure to set the header 'Authorization' with a valid token", 401)
        if header == api_token:
            return function(*args, **kwargs)
        else:
            return Response("Authorization token is invalid", 401)

    return decorated
