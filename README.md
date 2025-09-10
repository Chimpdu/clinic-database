# First, download dependency from requirement.txt
# Second, run bootstrap_db.py to create the database. Login postgre by providing your password in the commandline.
# Third, run login.py to login
# Default "admin" password "admin" default normal user "user1" password "user123"
# The files uploaded are stored using Large Object. To export the stored files, one can use the following command in PSQL tool: 
# \\\! mkdir C:\temp
# \lo_export 25477 'C:/temp/filename'
