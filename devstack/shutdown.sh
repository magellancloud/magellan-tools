#!/bin/bash
# Shut down the currently running devstack
# This assumes that the OpenStack processes are running
# under the "stack" user in a screen session
CWD=`pwd`
# Kill the running screen
su stack -c "screen -x stack -X quit"
# Run the unstack.sh program
if [[ ! -d $CWD/devstack ]] ; then
    echo "No devstack found in $CWD";
    exit 1;
fi
pushd $CWD/devstack
# Below taken from https://bugs.launchpad.net/devstack/+bug/1033573
# And needed for switching between essex and folsom

    # clean up apache config
    # essex devstack uses 000-default
    # folsom devstack uses horizon -> ../sites-available/horizon
    if [[ -e /etc/apache2/sites-enabled/horizon ]]; then
        # Clean up folsom-style
        sudo a2dissite horizon
        sudo service apache2 reload
    fi

# Now run standard unstack.sh script
if [[ -f "unstack.sh" ]] ; then
    ./unstack.sh
fi
popd
