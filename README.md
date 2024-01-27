Welcome to the RedShift Workspace of Darius

# Installation
I assume you run this on ubuntu, locally with Python 3.10 (ubuntu default)
```shell
python3 -m venv ./venv
. venv/bin/activate
pip3 install -r requirements.txt
echo "[AWS]" > dwh.cfg
echo "KEY=<KEY>" >> dwh.cfg
echo "SECRET=<SECRET>" >> dwh.cfg
```

        Make sure that you have the virtual environment active before you continue



# Pre-Requirements
This section describe that you need to run, 
in order to get a cluster up and running.


```shell
python3 create_redshift.py
```

This script will create the redshift that can be accessed 
from the ip that executes this script.

This will also extend the dwh.cfg file with the required informations
to 

# create_tables_py
This script will create the tables that are required to complete the project.
