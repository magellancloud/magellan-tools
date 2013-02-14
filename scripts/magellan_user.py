#!/usr/bin/env python
import argparse
import os
import sys
from keystoneclient.exceptions import NotFound
from keystoneclient.v2_0 import client

parser = argparse.ArgumentParser(
    description="""
Update a user in Magellan.

If the tenant does not exist this will return an error. If the user
does not exist, he or she will be added to the system iff a password
is supplied.
"""
)
parser.add_argument('-u', dest='user',
    help="The username for the user, not the user_id.",
    type=str, required=True)
parser.add_argument('-t', dest='tenant',
    help="The tenant name, not the tenant_id.", type=str)
parser.add_argument('--is_admin', action='store_const', const=True,
    help="Ensure that the user has 'admin' privileges on the tenant.")
parser.add_argument('--disable', action='store_const', const=True,
    help="Disable the user.")
parser.add_argument('--evict', action='store_const', const=True,
    help="Remove the user from that tenant, removing both admin and non-admin privileges")
parser.add_argument('--delete', action='store_const', const=True,
    help="Delete the user completely")
parser.add_argument('-p', dest='password', type=str,
    help="Set the user's password.")
parser.add_argument('-e', dest='email', type=str,
    help="Set the user's email.")
parser.add_argument('--insecure', action='store_const', const=True,
    help="Do not validate SSL Certificate when connecting to Keystone"
)

args = parser.parse_args()
env = os.environ
keystone = client.Client(
    username =    env['OS_USERNAME'],
    password =    env['OS_PASSWORD'],
    tenant_name = env['OS_TENANT_NAME'],
    auth_url =    env['OS_AUTH_URL'],
    insecure =    args.insecure,
)
# Get standard admin and member roles:
member_role = [ r for r in keystone.roles.list() if r.name == "Member"][0]
admin_role  = [ r for r in keystone.roles.list() if r.name == "admin"][0] 

def get_user (name, keystone):
    matched = [ user for user in keystone.users.list() if user.name == args.user]
    return matched[0] if len(matched) else None

def delete_user (keystone, args):
    user = get_user(args.user, keystone) 
    user.delete()

def ensure_tenant (keystone, args):
    matched = [ tenant for tenant in keystone.tenants.list()
        if tenant.tenant_name == args.tenant ]
    if len(matched):
        return matched[0]
    else:
        return keystone.tenants.create(args.tenant)

def ensure_user (keystone, args):
    user = get_user(args.user, keystone)
    # If no user was found, create the user with password and email
    if user is None:
        if args.password is None:
            print >> sys.stderr, "User not found and password is not supplied!"
            sys.exit(1)
        keystone.users.create(
            name     = args.user,
            password = args.password,
            email    = args.email,
            enabled  = False if args.disable else True
        )
    elif args.password:
       keystone.users.update_password(user, args.password)
    elif args.disable:
        keystone.users.update_enabled(user, False if args.disable else True)

def update_user_tenant (keystone, args):
    user = get_user(args.user, keystone)
    tenant = [ t for t in keystone.tenants.list() if t.name == args.tenant ][0]
    if tenant is None:
        # Exit with an error if we couldn't find the tenant
        print >> sys.stderr, "Tenant not found!"
        sys.exit(1)
    # Remove the admin role for the user if we did not set the admin flag
    if not args.is_admin:
        try:
            keystone.roles.remove_user_role(user, admin_role, tenant=tenant)
        except NotFound:
            pass
    if args.evict:
        # Evicting the user from the tenant
        try:
            keystone.roles.remove_user_role(user, admin_role, tenant=tenant)
        except NotFound:
            pass
        try:
            keystone.roles.remove_user_role(user, member_role, tenant=tenant)
        except NotFound:
            pass
    else:
        # Adding the user to the tenant with the correct role
        role = admin_role if args.is_admin else member_role
        try:
            keystone.roles.add_user_role(user, role, tenant=tenant)
        except NotFound:
            pass
        
if args.delete:
    delete_user(keystone, args)
else:
    ensure_tenant(keystone, args)
    ensure_user(keystone, args)
    if args.tenant != None:
        update_user_tenant(keystone, args)
