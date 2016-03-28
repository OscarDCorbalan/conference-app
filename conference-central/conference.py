#!/usr/bin/env python

"""
conference.py -- Udacity conference server-side Python App Engine API;
    uses Google Cloud Endpoints
"""

__author__ = 'oscardc@gmx.com (Oscar D. Corbalan)'

# TODO join imports
from datetime import datetime
import json
import os
import time

import endpoints
from protorpc import messages
from protorpc import message_types
from protorpc import remote

from google.appengine.api import urlfetch
from google.appengine.ext import ndb

from models import (Profile, ProfileMiniForm, ProfileForm, TeeShirtSize,
    Conference, ConferenceForm, ConferenceForms, ConferenceQueryForm, 
    ConferenceQueryForms, BooleanMessage, ConflictException)

from settings import WEB_CLIENT_ID

from utils import get_user_id


EMAIL_SCOPE = endpoints.EMAIL_SCOPE
API_EXPLORER_CLIENT_ID = endpoints.API_EXPLORER_CLIENT_ID

DEFAULTS = {
    "city": "Default City",
    "maxAttendees": 0,
    "seatsAvailable": 0,
    "topics": [ "Default", "Topic" ],
}

CONF_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
)

# Query filtering constants
OPERATORS = {
            'EQ':   '=',
            'GT':   '>',
            'GTEQ': '>=',
            'LT':   '<',
            'LTEQ': '<=',
            'NE':   '!='
            }

FIELDS =    {
            'CITY': 'city',
            'TOPIC': 'topics',
            'MONTH': 'month',
            'MAX_ATTENDEES': 'maxAttendees',
            }

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

@endpoints.api( name='conference',
                version='v1',
                allowed_client_ids=[WEB_CLIENT_ID, API_EXPLORER_CLIENT_ID],
                scopes=[EMAIL_SCOPE])
class ConferenceApi(remote.Service):
    """Conference API v0.1"""

    @endpoints.method(message_types.VoidMessage, ProfileForm,
        path='profile', http_method='GET', name='getProfile')
    def _get_profile(self, request):
        """Return user profile."""
        return self._do_profile()


    @endpoints.method(ProfileMiniForm, ProfileForm,
        path='profile', http_method='POST', name='saveProfile')
    def _save_profile(self, request):
        """Update & return user profile."""
        return self._do_profile(request)


    @endpoints.method(ConferenceForm, ConferenceForm,
        path='conference', http_method='POST', name='createConference')
    def _create_conference(self, request):
        """Create new conference."""
        return self._create_conference_object(request)


    @endpoints.method(ConferenceQueryForms, ConferenceForms,
        path='queryConferences', http_method='POST', name='queryConferences')
    def query_conferences(self, request):
        """Query for conferences."""
        conferences = self._get_query(request)

         # return individual ConferenceForm object per Conference
        return ConferenceForms(
            items=[self._copy_conference_to_form(conf, "") \
            for conf in conferences]
        )


    @endpoints.method(message_types.VoidMessage, ConferenceForms,
        path='getConferencesCreated', http_method='POST', 
        name='getConferencesCreated')
    def get_conferences_created(self, request):
        """Return conferences created by user."""
        # make sure user is authed
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        # make profile key
        p_key = ndb.Key(Profile, get_user_id(user))
        # create ancestor query for this user
        conferences = Conference.query(ancestor=p_key)
        # get the user profile and display name
        prof = p_key.get()
        displayName = getattr(prof, 'displayName')
        # return set of ConferenceForm objects per Conference
        return ConferenceForms(
            items=[self._copy_conference_to_form(
                conf, displayName) for conf in conferences]
        )
    

    @endpoints.method(CONF_GET_REQUEST, ConferenceForm,
        path='conference/{websafeConferenceKey}', http_method='GET', 
        name='getConference')
    def get_conference(self, request):
        """Return requested conference (by websafeConferenceKey)."""
        # get Conference object from request; bail if not found
        conf = ndb.Key(urlsafe=request.websafeConferenceKey).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % request.websafeConferenceKey)
        prof = conf.key.parent().get()
        # return ConferenceForm
        return self._copy_conference_to_form(conf, getattr(prof, 'displayName'))


    @endpoints.method(CONF_GET_REQUEST, BooleanMessage,
            path='conference/{websafeConferenceKey}',
            http_method='POST', name='registerForConference')
    def register_for_conference(self, request):
        """Register user for selected conference."""
        return self._conference_registration(request)


    @endpoints.method(message_types.VoidMessage, ConferenceForms,
        path='conferences/attending', http_method='GET',
        name='getConferencesToAttend')
    def get_conferences_to_attend(self, request):
        """Get list of conferences that user has registered for."""
        # TODO:
        # Get user Profile
        profile = self._get_profile_from_user()
        # step 2: get conferenceKeysToAttend from profile.
        # to make a ndb key from websafe key you can use:
        # ndb.Key(urlsafe=my_websafe_key_string)
        ws_keys = profile.conferenceKeysToAttend
        ndb_keys = [ndb.Key(urlsafe = ws_key) for ws_key in ws_keys]
        # step 3: fetch conferences from datastore. Do not fetch them one by one!
        conferences = ndb.get_multi(ndb_keys)

        # return set of ConferenceForm objects per Conference
        return ConferenceForms(items = [
            self._copy_conference_to_form(conf, "") for conf in conferences])


    # - - - - - - - - - - - -  - - - - - - - - - - - - - - - - - - - - - - - -

    def _copy_profile_to_form(self, prof):
        """Copy relevant fields from Profile to ProfileForm."""
        # copy relevant fields from Profile to ProfileForm
        pf = ProfileForm()
        for field in pf.all_fields():
            if hasattr(prof, field.name):
                # convert t-shirt string to Enum; just copy others
                if field.name == 'teeShirtSize':
                    setattr(pf, field.name, getattr(
                        TeeShirtSize, getattr(prof, field.name)))
                else:
                    setattr(pf, field.name, getattr(prof, field.name))
        pf.check_initialized()
        return pf


    def _copy_conference_to_form(self, conf, displayName):
        """Copy relevant fields from Conference to ConferenceForm."""
        cf = ConferenceForm()
        for field in cf.all_fields():
            if hasattr(conf, field.name):
                # convert Date to date string; just copy others
                if field.name.endswith('Date'):
                    setattr(cf, field.name, str(getattr(conf, field.name)))
                else:
                    setattr(cf, field.name, getattr(conf, field.name))
            elif field.name == "websafeKey":
                setattr(cf, field.name, conf.key.urlsafe())
        if displayName:
            setattr(cf, 'organizerDisplayName', displayName)
        cf.check_initialized()
        return cf


    def _get_profile_from_user(self):
        """Return user Profile from datastore, creating new one if 
        non-existent."""
        # Make sure user is authed
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        

        # Try to retrieve an existing profile...
        user_id = get_user_id(user)
        key = ndb.Key(Profile, user_id)
        profile = key.get()
        # ... and if not exists, create a new Profile from logged in user data
        if not profile:
            profile = Profile(
                userId = user_id,
                key = key,
                displayName = user.nickname(), 
                mainEmail= user.email(),
                teeShirtSize = str(TeeShirtSize.NOT_SPECIFIED),
            )
            # Create the profile in datastore
            profile.put()

        return profile


    def _do_profile(self, save_request=None):
        """Get user Profile and return to user, possibly updating it first."""
        # get user Profile
        prof = self._get_profile_from_user()

        # if _save_profile(), process user-modifyable fields
        if save_request:
            for field in ('displayName', 'teeShirtSize'):
                if hasattr(save_request, field):
                    val = getattr(save_request, field)
                    if val:
                        setattr(prof, field, str(val))
            # Save changes into datastore
            prof.put()

        # return ProfileForm
        return self._copy_profile_to_form(prof)


    def _create_conference_object(self, request):
        """Create or update Conference object, returning 
        ConferenceForm/request."""
        # Preload necessary data items
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = get_user_id(user)

        if not request.name:
            raise endpoints.BadRequestException(
                "Conference 'name' field required")

        # Copy ConferenceForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name) 
                for field in request.all_fields()}
        del data['websafeKey']
        del data['organizerDisplayName']

        # Add default vals for those missing (in data model & outbound Message)
        for df in DEFAULTS:
            if data[df] in (None, []):
                data[df] = DEFAULTS[df]
                setattr(request, df, DEFAULTS[df])

        # Convert dates from strings to Date obj; set month based on start_date
        if data['startDate']:
            data['startDate'] = datetime.strptime(
                data['startDate'][:10], "%Y-%m-%d").date()
            data['month'] = data['startDate'].month
        else:
            data['month'] = 0
        if data['endDate']:
            data['endDate'] = datetime.strptime(
                data['endDate'][:10], "%Y-%m-%d").date()

        # set seatsAvailable to be same as maxAttendees on creation
        # both for data model & outbound Message
        if data["maxAttendees"] > 0:
            data["seatsAvailable"] = data["maxAttendees"]
            setattr(request, "seatsAvailable", data["maxAttendees"])

        # make Profile Key from user ID
        p_key = ndb.Key(Profile, user_id)
        # allocate new Conference ID with Profile key as parent
        c_id = Conference.allocate_ids(size=1, parent=p_key)[0]
        # make Conference key from ID
        c_key = ndb.Key(Conference, c_id, parent=p_key)
        data['key'] = c_key
        data['organizerUserId'] = request.organizerUserId = user_id

        # create Conference & return (modified) ConferenceForm
        Conference(**data).put()

        return request
    

    def _get_query(self, request):
        """Return formatted query from the submitted filters."""
        q = Conference.query()
        inequality_filter, filters = self._format_filters(request.filters)

        # If exists, sort on inequality filter first
        if not inequality_filter:
            q = q.order(Conference.name)
        else:
            q = q.order(ndb.GenericProperty(inequality_filter))
            q = q.order(Conference.name)

        for filtr in filters:
            if filtr["field"] in ["month", "maxAttendees"]:
                filtr["value"] = int(filtr["value"])
            formatted_query = ndb.query.FilterNode(
                filtr["field"], filtr["operator"], filtr["value"])
            q = q.filter(formatted_query)
        return q


    def _format_filters(self, filters):
        """Parse, check validity and format user supplied filters."""
        formatted_filters = []
        inequality_field = None

        for f in filters:
            filtr = {field.name: getattr(f, field.name) 
                     for field in f.all_fields()}

            try:
                filtr["field"] = FIELDS[filtr["field"]]
                filtr["operator"] = OPERATORS[filtr["operator"]]
            except KeyError:
                raise endpoints.BadRequestException(
                    "Filter contains invalid field or operator.")

            # Every operation except "=" is an inequality
            if filtr["operator"] != "=":
                # Check if inequality operation has been used in previous
                # filters. Disallow the filter if inequality was performed on a
                # different field before. Track the field on which the
                # inequality operation is performed
                if inequality_field and inequality_field != filtr["field"]:
                    raise endpoints.BadRequestException(
                        "Inequality filter is allowed on only one field.")
                else:
                    inequality_field = filtr["field"]

            formatted_filters.append(filtr)
        return (inequality_field, formatted_filters)
    

    @ndb.transactional(xg=True)
    def _conference_registration(self, request, reg=True):
        """Register or unregister user for selected conference."""
        retval = None
        prof = self._get_profile_from_user() # get user Profile

        # Check if conf exists given websafeConfKey
        # Get conference; check that it exists
        wsck = request.websafeConferenceKey
        conf = ndb.Key(urlsafe=wsck).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % wsck)

        # Register
        if reg:
            # Check if user already registered otherwise add
            if wsck in prof.conferenceKeysToAttend:
                raise ConflictException(
                    "You have already registered for this conference")

            # Check if seats available
            if conf.seatsAvailable <= 0:
                raise ConflictException(
                    "There are no seats available.")

            # Register user, take away one seat
            prof.conferenceKeysToAttend.append(wsck)
            conf.seatsAvailable -= 1
            retval = True

        # Unregister
        else:
            # Check if user already registered
            if wsck in prof.conferenceKeysToAttend:

                # Unregister user, add back one seat
                prof.conferenceKeysToAttend.remove(wsck)
                conf.seatsAvailable += 1
                retval = True
            else:
                retval = False

        # Write things back to the datastore & return
        prof.put()
        conf.put()
        return BooleanMessage(data=retval)

# - - - - - - - - - - - - - - - - - - - - - 

    @endpoints.method(message_types.VoidMessage, ConferenceForms,
            path='filterPlayground',
            http_method='GET', name='filterPlayground')
    def filterPlayground(self, request):
        q = Conference.query()

        # simple filter
        q = q.filter(Conference.city == "London")
        
        # Equivalent through ndb
        # field = "city"
        # operator = "="
        # value = "London"
        # f = ndb.query.FilterNode(field, operator, value)
        # q = q.filter(f)

        q = q.filter(Conference.topics == "Medical Innovations")
        
        q = q.order(Conference.name)
        
        q = q.filter(Conference.maxAttendees > 10)

        return ConferenceForms(
            items=[self._copy_conference_to_form(conf, "") for conf in q]
        )


# registers API
api = endpoints.api_server([ConferenceApi]) 
