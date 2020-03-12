# Postgres source of truth

This database stores data exported by the scanner pipeline and used to back the 
TV news viewer. It has both import and export facilities. The import script takes
data exported from the old database and migrates it. It is mostly single use.
The export script produces an summary of the database in a format the viewer 
can read.

## Non default postgres parameters

### postgresql.conf 

This setting makes the database run on the SSD:

data_directory = '/newdisk/postgres-data'

If you want to make the database accessible for other machines, you'll need to 
uncomment this line:

listen_addresses='\*'

You'll also need to add entries to pg_hba.conf.
