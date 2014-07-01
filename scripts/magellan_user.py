#!/usr/bin/env python
import argparse
import ConfigParser
import os
import subprocess
import sys
from keystoneclient.exceptions import NotFound
from keystoneclient.v2_0 import client

def main():
    desc = """Update a user in Magellan.

If the tenant does not exist this will return an error. If the user
does not exist, he or she will be added to the system iff a password
is supplied.
"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-u', dest='user',
                        help="The username for the user, not the user_id.",
                        type=str, required=True)
    parser.add_argument('-t', dest='tenant', type=str,
                        help="The tenant name, not the tenant_id.")
    parser.add_argument('--is-admin', action='store_const',
                        dest='is_admin', const=True, help="Ensure that "
                        "the user has 'admin' privileges on the tenant.")
    parser.add_argument('--disable', action='store_const', const=True,
                        help="Disable the user.")
    parser.add_argument('--evict', action='store_const', const=True,
                        help="Remove the user from that tenant, removing "
                        "both admin and non-admin privileges")
    parser.add_argument('--delete', action='store_const', const=True,
                        help="Delete the user completely")
    parser.add_argument('-p', dest='password', type=str,
                        help="Set the user's password.")
    parser.add_argument('-e', dest='email', type=str,
                        help="Set the user's email.")
    parser.add_argument('--insecure', action='store_const', const=True,
                        help="Do not validate SSL Certificate when "
                        "connecting to Keystone")
    args = parser.parse_args()

    # We pull all environment variables from a config file.
    # Each section designates a separate OpenStack env that we must
    # independently update.
    cfg_file = os.path.expanduser("~/userbase.config")
    if not os.path.exists(cfg_file):
        print "Config file %s does not exist!" % cfg_file
        sys.exit(1)
    cfg = ConfigParser.ConfigParser()
    cfg.read(cfg_file)
    for section in cfg.sections():
        apply_settings_for_os_env(dict(cfg.items(section)), args)

def apply_settings_for_os_env(cfg, args):
    for k,v in cfg.iteritems():
        os.environ[k] = v
    keystone = client.Client(
        username =    cfg.get("username"),
        password =    cfg.get("password"),
        tenant_name = cfg.get("tenant_name"),
        auth_url =    cfg.get("auth_url"))
    # Get standard admin and member roles:
    admin_role_name = os.environ.get('admin_role', 'admin')
    member_role = [r for r in keystone.roles.list() if r.name == "Member"][0]
    admin_role  = [r for r in keystone.roles.list() if
                   r.name == admin_role_name][0] 
    if args.delete:
        delete_user(keystone, args)
    else:
        ensure_tenant(keystone, args)
        ensure_user(keystone, args)
        if args.tenant != None:
            update_user_tenant(keystone, args, admin_role=admin_role, member_role=member_role)

def get_user (name, keystone):
    matched = [u for u in keystone.users.list() if u.name == name]
    return matched[0] if len(matched) else None

def delete_user(keystone, args):
    user = get_user(args.user, keystone) 
    user.delete()

def ensure_tenant(keystone, args):
    if not args.tenant:
        return
    matched = [ tenant for tenant in keystone.tenants.list()
        if tenant.name == args.tenant ]
    if len(matched):
        return matched[0]
    else:
        return keystone.tenants.create(args.tenant)

def ensure_user(keystone, args):
    user = get_user(args.user, keystone)
    # If no user was found, create the user with password and email
    if user is None:
        if args.password is None:
            print "User not found and password is not supplied!"
            sys.exit(1)
        keystone.users.create(
            name     = args.user,
            password = args.password,
            email    = args.email,
            enabled  = False if args.disable else True
        )
    else:
        keystone.users.update_enabled(user, False if args.disable else True)
        if args.password:
            keystone.users.update_password(user, args.password)
        if args.email:
            keystone.users.update(user, email=args.email)

def update_user_tenant(keystone, args, admin_role=None, member_role=None):
    user = get_user(args.user, keystone)
    tenant = [ t for t in keystone.tenants.list() if t.name == args.tenant ][0]
    if tenant is None:
        # Exit with an error if we couldn't find the tenant
        print "Tenant not found!"
        sys.exit(1)
    # Remove the admin role for the user if we did not set the admin flag
    #if not args.is_admin:
    #    try:
    #        keystone.roles.remove_user_role(user, admin_role, tenant=tenant)
    #    except Exception:
    #        pass
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
        ub_user = os.environ.get('username')
        ub_pass = os.environ.get('password')
        ub_tenant = os.environ.get('tenant_name')
        auth_url = os.environ.get('auth_url')
        try:
            subprocess.check_output(["keystone", "--os_username", ub_user, "--os_password", ub_pass,
                                     "--os_tenant_name", ub_tenant, "--os_auth_url", auth_url,
                                     "user-role-add", "--user", user.id, "--tenant_id", tenant.id,
                                     "--role", role.id])
        except Exception, e:
            print "Error adding %s user to %s tenant: %s" % (user.name, tenant.name, e)
            sys.exit(1)

if __name__ == '__main__':
    main()
