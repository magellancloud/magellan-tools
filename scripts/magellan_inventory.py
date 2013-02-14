#!/usr/bin/env python
import argparse
import json
import os
import sys
from keystoneclient.exceptions import NotFound
from keystoneclient.v2_0 import client

parser = argparse.ArgumentParser(
    description="""
Return an inventory of Magellan tenants and users as a JSON data structure.
This is printed to standard output with the following format:
{
    "tenants" : {
        "tenant_name" : {
            "description" : "blah blah blah!",
            enabled : True
        }
    },
    users : {
        "user_name" : {
            "email" : "foo@example.com",
            "active" : True,
         }
    },
    memberships : {
        "tenant_name" : {
            "user_one" : "admin",
            "user_two" : "Member"
        },
        "tenant_two" : {
            "user_two" : "admin"
        }
    }
}
"""
)
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

tenants = { (tenant.tenant_name, { "description" : tenant.description, "enabled" : tenant.enabled }) for tenant in keystone.tenants.list() }
users   = keystone.users.list()
memberships = {}
data = { "tenants" : tenants, "users" : users, "memberships" : memberships }
print json.dumps(data)
