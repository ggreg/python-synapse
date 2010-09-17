#
# Regular cron jobs for the python-synapse package
#
0 4	* * *	root	[ -x /usr/bin/python-synapse_maintenance ] && /usr/bin/python-synapse_maintenance
