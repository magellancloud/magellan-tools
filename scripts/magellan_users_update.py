#!/usr/bin/env python
""" Update magellan-users@lists.mcs.anl.gov to contain all current
    and active users. Currently this is though a hacky parsing of
    the Mailman web interface."""

import BeautifulSoup
import ConfigParser
import cookielib
import os
import re
import sys
import urllib
import urllib2
from keystoneclient.v2_0.client import Client as KeystoneClient


def _get_opener():
    cj = cookielib.CookieJar()
    return urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))


def _rsp_soup(rsp):
    rsp = rsp.read()
    return BeautifulSoup.BeautifulSoup(rsp)


class MailMan(object):
    """Wrapper for login information and sub/unsub functions."""
    def __init__(self, host, listname, admin_password, http_schema='https', proxy=None):
        self.host = host
        self.listname = listname
        self.http_schema = http_schema
        self.opener = None
        self.proxy = proxy
        self.do_login(admin_password)

    def do_login(self, password):
        cj = cookielib.CookieJar()
        if self.proxy:
	    proxy_handler = urllib2.ProxyHandler({'https': self.proxy})
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj), proxy_handler)
        else:
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        login_data = urllib.urlencode({'adminpw': password})
        url = '%s://%s/mailman/admin/%s' % (self.http_schema,
                                            self.host, self.listname)
        rsp = opener.open(url, login_data)
        if rsp.code != 200:
            raise Exception("Error logging into %s list %s: %s" %
                            (self.host, self.listname, rsp.code))
        self.opener = opener
        return rsp.code

    def get_users(self):
        # Mailman has awesome alphabetical pagination.
        # So look for links ending in ?letter=x and add those as additional
        # pages that we need to look at. Maintain a queue and dictionary of seen
        # urls. For the remainder of the links in the table, we can assume that
        # they contain email addresses.
        url = '%s://%s/mailman/admin/%s/members/list' % (self.http_schema, self.host, self.listname)
        to_query = [url]
        queried = {}
        users = []
        while len(to_query) > 0:
            url = to_query.pop()
            queried[url] = True
	    soup = _rsp_soup(self.opener.open(url))
            member_list_table = soup.findAll('table')[4]
	    for a in member_list_table.findAll('a'):
                href = a.get('href')
	        if re.match(".*?letter=.", href):
		    if href not in to_query and href not in queried:
		        to_query.append(href)
                else:
                    users.append(a.text)
        return users

    def add_users(self, user_emails):
        url = '%s://%s/mailman/admin/%s/members/add'
        url = url % (self.http_schema, self.host, self.listname)
        newline_delimited_list = "\n".join(user_emails)
        data = {'subscribees': newline_delimited_list,
                'send_notifications_to_list_owner': 1,
                'send_welcome_msg_to_this_batch': 1,
                'subscribe_or_invite': 0}
        data = urllib.urlencode(data)
        return self.opener.open(url, data)

    def remove_users(self, user_emails):
        url = '%s://%s/mailman/admin/%s/members/remove'
        url = url % (self.http_schema, self.host, self.listname)
        newline_delimited_list = "\n".join(user_emails)
        data = {'unsubscribees': newline_delimited_list,
                'send_unsub_notifications_to_list_owner': 1,
                'send_unsub_ack_to_this_batch': 0}
        data = urllib.urlencode(data)
        return self.opener.open(url, data)


def _get_config(filename):
    filename = os.path.abspath(filename)
    if not os.path.exists(filename):
        print "Config file %s does not exist!" % filename
        sys.exit(1)
    config = ConfigParser.ConfigParser()
    config.read(filename)
    return config


def get_keystone(config, section):
    kwargs = dict([(o, config.get(section, o))
                   for o in config.options(section)])
    return KeystoneClient(**kwargs)


def main():
    config = _get_config(sys.argv[1])
    mm = MailMan(config.get('list', 'host'),
                 config.get('list', 'listname'),
                 config.get('list', 'password'),
                 proxy=config.get('list', 'proxy'))
    keystone = get_keystone(config, 'keystone')
    os_users = [u.email for u in keystone.users.list() if u.email is not None]
    blacklist = [s for s in config.get('default', 'blacklist', '').split(',')
                 if s != '']
    whitelist = [s for s in config.get('default', 'whitelist', '').split(',')
                 if s != '']
    to_add = []
    to_remove = []
    mm_users = mm.get_users()
    for user in mm_users:
        if user not in os_users and user not in whitelist:
            to_remove.append(user)
    for user in os_users:
        if user not in mm_users and user not in blacklist:
            to_add.append(user)
    # Add users
    mm.add_users(to_add)
    # TODO: remove users
    mm.remove_users(to_remove)
 


if __name__ == '__main__':
    main()
