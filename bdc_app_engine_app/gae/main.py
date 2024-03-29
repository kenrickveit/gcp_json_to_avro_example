#!/usr/bin/env python
#
# Copyright 2015 Google Inc.
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

import webapp2

from pubsub_utils import publish_to_topic

EVENTS_PREFIX = "/events/"


class MainHandler(webapp2.RequestHandler):
    def get(self):
        self.response.write("<h1>Duracron!</h1>")


class CronEventHandler(webapp2.RequestHandler):
    def get(self):
        topic_name = self.request.path.split(EVENTS_PREFIX)[-1]
        publish_to_topic(topic_name, msg='json_to_avro_convert')
        self.response.status = 204


app = webapp2.WSGIApplication([('/', MainHandler),
                               ('/events/.*', CronEventHandler), ],
                              debug=True)
