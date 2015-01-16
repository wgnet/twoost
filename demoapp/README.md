## twoost-based project
This is the source code of 'demoapp'

## Project structure

- bin/ -- init scripts & runnable tools
- conf/ -- configs OR config templates
- scripts/ -- various python scripts
- migrations/ -- DB migrations
- src/demoapp/ -- application source
- src/demoapp/tests/ -- unit tests


## How to run.

Prepare env:

    # copy dev configs
    mkdir -p ~/conf
    cp -r ./conf/dev/ ~/conf

    # link nginx config
    sudo sh -c "echo 'include $HOME/conf/nginx/*.conf;' > /etc/nginx/conf.d/$USER.conf"
    sudo service nginx reload

    # init db, you *should* use migration tools in real apps
    sqlite3 ~/demoapp_dev.db < "migrations/0001_init.sql"


Start/stop app:

    ./bin/demoapp start


Check app status:

    ./bin/demoapp status
    # ..or..
    ./bin/demoapp info --all


## How to use.


Simple rpc methods:

    curl --header "content-type:application/json" \
        --data '[]' \
        'http://demoapp.local/demoapp/rpc/dumbrpc?method=version'

    curl --header "content-type:application/json" \
        --data '["any-string"]' \
        'http://demoapp.local/demoapp/rpc/dumbrpc?method=echo'


Insert new items to db:

    curl --header "content-type:application/json" \
        --data '["event-data"]' \
        'http://demoapp.local/demoapp/rpc/dumbrpc?method=new_event'


Use one-shot scripts & tools:

    python scripts/print_last_events.py


## credentials

Anonymous Coder <email@example.com>

