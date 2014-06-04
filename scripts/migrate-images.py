#!/usr/env/bin python
import argparse
import ConfigParser
import logging
import os
import sys
from keystoneclient.v2_0.client import Client as KeystoneClient
from glanceclient.v1.client import Client as GlanceV1Client
from glanceclient.v2.client import Client as GlanceV2Client

def do_sync(source, destination, image_cache):
     for image in source.glance.images.list(is_public=None):
         pass

class UserMap(object):
    # TODO(devoid): Need to make this multiple-deployment capable
    def __init__(self, source, destination):
         self.s2d = {}
         self.d2s = {}
         src_k = source.keystone
         dst_k = destination.keystone
         src_users = dict([(u.name, u) for u in list(src_k.users.list())])
         dst_users = dict([(u.name, u) for u in list(dst_k.users.list())])
         for name, user in src_users.iteritems():
             if name in dst_users:
                 dst_id = dst_users[name].id
                 self.s2d[user.id] = dst_id
                 self.d2s[dst_id] = user.id
         for name, user in dst_users.iteritems():
             if user.id in self.d2s:
                 continue
             logging.info('User %s in %s not found in %s' % (name, destination.name, source.name))

class Deployment(object):
    def __init__(self, config, section):
         self.name = section
         k = KeystoneClient(username=config.get(section, 'username'),
                            password=config.get(section, 'password'),
                            tenant_name=config.get(section, 'tenant_name'),
                            auth_url=config.get(section, 'auth_url'))
         self.keystone = k 
         glance_endpoint = self._get_endpoint('image')
         self.glance = GlanceV1Client(glance_endpoint['internalURL'],
                                      token=self.keystone.auth_token)

    def _get_endpoint(self, endpoint_type):
         e = self.keystone.service_catalog.get_endpoints()
         if endpoint_type not in e:
              return None
         return e[endpoint_type][0]

def _get_config(filename):
    filename = os.path.abspath(filename)
    if not os.path.exists(filename):
         print "Config file %s does not exist!" % filename
         sys.exit(1)
    config = ConfigParser.ConfigParser()
    config.read(filename)
    return config

def _setup_image_cache(path):
    path = os.path.abspath(path)
    if not os.path.exists(path):
         os.makedirs(path)
    return path

def _setup_logging(logfile):
     if logfile:
          logging.basicConfig(filename=logfile, level=logging.INFO)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-c', type=str, required=True,
                        help='Config file for keystone logins.')
    parser.add_argument('--logfile', type=str,
                        help='Path to log file to use. Otherwise STDERR used.')
    parser.add_argument('--image-cache-dir', '-i', type=str, required=True,
                        help='Directory to cache images in before upload.')
    parser.add_argument('source', type=str, help='Where to copy images from.')
    parser.add_argument('destination', type=str, help='Where to copy images to.')
    args = parser.parse_args()
    config = _get_config(args.config)
    image_cache = _setup_image_cache(args.image_cache_dir)
    _setup_logging(args.logfile)
    source = Deployment(config, args.source)
    destination = Deployment(config, args.destination)
    UserMap(source, destination)
    do_sync(source, destination, image_cache)

if __name__ == '__main__':
    main()
