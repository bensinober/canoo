# canoo

WIP project for creating a mysql replication canal to items availability

Based on mysql replication tools and the brilliant https://github.com/siddontang/go-mysql
and the BoltDB

Requires mysqldump tool for initial sync

Usage of ./canoo:

    -host string
        MySQL host (default "127.0.0.1")
    -port int
        MySQL port (default 3306)
    -user string
        MySQL user, must have replication privilege (default "root")
    -password string
        MySQL password
    -server-id int
        Unique Server ID (default 101)
    -data-dir string
        Path to store data, like master.info (default "./tmp")
    -dbs string
        dump databases, seperated by comma (default "test")
    -flavor string
        Flavor: mysql or mariadb (default "mysql")
    -ignore_tables string
        ignore tables, must be database.table format, separated by comma
    -mysqldump string
        mysqldump execution path (default "mysqldump")
    -table_db string
        database for dump tables (default "test")
    -tables string
        dump tables, seperated by comma, will overwrite dbs

Example:

    ./canoo -user=dude -password=secret -table_db=mydb -tables=items,issues,reserves

starts a canoo between mysql on port 3306 on localhost, dumping the tables `items,issues,reserves`
and then starts listening for live events on said tables.