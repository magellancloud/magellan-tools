#!/usr/bin/env python
import argparse
import logging
import os
import re
import string
import sys

from novaclient.v1_1 import client as nova_client
from keystoneclient.v2_0 import client as keystone_client

# Supress all logging for kyestone loggers
logger_names = [ "keystoneclient.client", "keystoneclient.v2_0.client" ]
for name in logger_names:
    _logger = logging.getLogger(name)
    _logger.addHandler(logging.NullHandler())

parser = argparse.ArgumentParser(
    description="""
Add or remove a key for a user
"""
)
parser.add_argument('-u', dest='user',
    help="The username for the user, not the user_id.",
    type=str, required=True)
parser.add_argument('-p', dest='password', type=str,
    help="Set the user's password.", required=True)
parser.add_argument('-n', dest='name', type=str,
    help="Name to use for the key", required=True)
parser.add_argument('-r', dest='rename', type=str,
    help="If this is set, rename key matching 'name' to 'rename' value.")
parser.add_argument('--delete', action='store_const', 
        const=True, help="Delete the key")
parser.add_argument('--insecure', action='store_const', const=True,
    help="Do not validate SSL Certificate when connecting to Keystone"
)
parser.add_argument('key_parts', nargs=argparse.REMAINDER)

args = parser.parse_args()
key_string = " ".join(args.key_parts) + '\n'

def fix_key_name(name):
    rx = re.compile('\W')
    def replace(match):
        return '-' if match.group(0) == '-' else ''
    fixed_name = rx.sub(replace, name)
    return fixed_name if len(fixed_name) else 'default' 

def ensure_key (nova, args, key_string):
    args.name = fix_key_name(args.name)
    matches = [ key for key in nova.keypairs.list() if key.name == args.name ]
    key_exists = True if len(matches) else False
    if args.delete and key_exists:
        nova.keypairs.delete(args.name)
    elif args.delete:
        pass
    elif args.rename and key_exists:
        key = matches[0]
        key_string = key.public_key
        nova.keypairs.create(args.rename, public_key=key_string)
        nova.keypairs.delete(args.name)
    elif args.rename:
        print "No key found with name " + args.name
    elif key_exists:
        nova.keypairs.delete(args.name)
        nova.keypairs.create(args.name, public_key=key_string)
    else:
        nova.keypairs.create(args.name, public_key=key_string)

def get_nova(auth_url, region, args):
    # Nova API requires a tenant so first get that
    # via keystone
    keystone = keystone_client.Client(
        username=args.user,
        password=args.password,
        auth_url=auth_url)
    tenants = keystone.tenants.list()
    if len(tenants) > 0:
        return nova_client.Client(args.user, args.password,
                tenants[0].name, auth_url, region_name=region, endpoint_type='internalURL', service_type='compute')
    else:
        sys.exit(0)

auth_url = os.environ['OS_AUTH_URL']
region   = os.environ['OS_REGION_NAME']
nova = get_nova(auth_url, region, args)
ensure_key(nova, args, key_string)
