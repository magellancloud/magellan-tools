#!/usr/bin/env python
import argparse
import sys

from novaclient.v1_1 import client

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
key_string = " ".join(args.key_parts)

def ensure_key (nova, args):
    matches = [ key for key in nova.keypairs.list() if key.name == args.name ]
    key_exists = True if len(matches) else False
    if args.delete:
        nova.keypairs.delete(args.name)
    elif args.rename and key_exists:
        key = matches[0]
        key_string= key.public_key
        nova.keypairs.create(args.rename, public_key=key_string)
        nova.keypairs.delete(args.name)
    elif args.rename:
        print >> sys.stderr, "No key found with name " + args.name
    elif not key_exists:
        nova.keypairs.create(args.name, public_key=key_string)

nova = client.Client(args.user, args.password)
ensure_key(nova, args)
