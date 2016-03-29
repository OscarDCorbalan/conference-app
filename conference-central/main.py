#!/usr/bin/env python
"""
main.py -- Conference server-side Python App Engine HTTP controller handlers
 for memcache

"""

__author__ = 'oscardc@gmx.com (Oscar D. Corbalan)'

import webapp2
from conference import ConferenceApi
from google.appengine.api import app_identity, mail

class SetAnnouncementHandler(webapp2.RequestHandler):
    def get(self):
        """Set Announcement in Memcache."""
        ConferenceApi._cache_announcement()
        self.response.set_status(204)


app = webapp2.WSGIApplication([
    ('/crons/set_announcement', SetAnnouncementHandler),
], debug=True)
