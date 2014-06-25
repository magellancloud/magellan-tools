""" Update magellan-users@lists.mcs.anl.gov to contain all current
    and active users. Currently this is though a hacky parsing of
    the Mailman web interface."""

import sys
import BeautifulSoup
import ConfigParser
import cookielib
import os
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
    def __init__(self, host, listname, admin_password, http_schema='https'):
        self.host = host
        self.listname = listname
        self.http_schema = http_schema
        self.opener = None
        self.do_login(admin_password)

    def do_login(self, password):
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        login_data = urllib.urlencode({'adminpw': password})
        url = '%s://%s/mailman/admin/%s' % (self.http_schema,
                                            self.host, self.listname)
        rsp = opener.open(url, login_data)
        if rsp != 200:
            raise Exception("Error logging into %s list %s: %s" %
                            (self.host, self.listname, rsp.code))
        self.opener = opener
        return rsp.code

    def get_users(self):
        url = '%s/mailman/admin/%s/members/list' % (self.host, self.listname)
        rsp = self.opener.open(url)
        soup = _rsp_soup(rsp)
        # Yes, select the 4th table because there is no class or id for this
        member_list_table = soup.findAll('table')[4]
        return list([a.text for a in member_list_table.findAll('a')])

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
                 config.get('list', 'password'))
    keystone = get_keystone(config, 'keystone')
    os_users = [u.name for u in keystone.users.list()]
    to_add = []
    to_remove = []
    mm_users = mm.get_users()
    for user in mm_users:
        if user not in os_users:
            to_remove.append(user)
    for user in os_users:
        if user not in mm_users:
            to_add.append(user)
    print "To add:"
    print "\n".join(to_add)
    print "To remove:"
    print "\n".join(to_remove)
    # TODO: Do remove step
    # TODO: Do add step


if __name__ == '__main__':
    main()
