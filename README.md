# mysql-data-migration
[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)

Python script to migrate logical data between tables and databases. 
I usually use this script to move certain data from one table to another table within a database or to another database in a different instance.
Your can read one of my use case in this [blog](https://medium.com/@mukaromalisyaban/mysql-boost-performance-through-data-retention-and-data-archiving-1-da2cba4e8a8e).

## Requirements
- Python 3+
- Hashicorp vault (for better security, but not mandatory)

## Features
- `migration.py` : the main script, used to migrate data from one table to another
- `migration_checker.py` : data checksum script, used to check if the data is 

## Usage
Clone the project
```bash
$ git clone https://github.com/maquebermans/mysql-data-migration.git
```

Go to the project directory
```bash
$ cd mysql-data-migration
```

Install dependencies

```bash
  $ source bin/activate
```

Activate virtual environment

```bash
  $ pip install -r requirement.txt
```

### File Description : 
1. `migration.json` : configuration file to control the `main script` behavior

2. `migration.py` : the `main script`

3. `migration_checker.json` : data checksum configuration, to control the `checksum main script` behavior

4. `migration_checker.py` : the `checksum main script`

5. `.vault` : this file is used to bypass hashicorp vault

### How to use :
1. We will bypass hashicorp vault first. Now you can open `.vault` file and fill the database credentials by changing x and y using database user and password accordingly.
```bash
v = "x,y"

x : database user
y : database password
```

2. Add table information to the `migration.json` file
```json
[
    {
        "source_db"         : "source_db",          //required | source db name
        "source_endpoint"   : "source_endpoint",    //required | source db ip/dns
        "source_table"      : "source_table",       //required | source table
        "target_db"         : "target_db",          //required | target db name
        "target_endpoint"   : "target_endpoint",    //required | target db ip/dns
        "target_table"      : "target_table",       //required | target table
        "column_key"        : "primary_key",        //required | use auto incremental key
        "column_date"       : "created_date",       //required | date column
        "min_date_period"   : "2025-02-01",         //required | minimum data to be moved
        "chunk_size"        : "10000",              //optional | number of records per cycle
        "row_auto_update"   : false,                //optional | enable/disable record auto update
        "is_active"         : false,                //required | enable/disable migration
        "is_autochecksum"   : false,                //optional | enable auto checksum
        "is_switched"       : false                 //optional | enable if the table has been switched
    },
    { // use this approach if you have multiple tables to migrate
        "source_db"         : "source_db",
        "source_endpoint"   : "source_endpoint",
        "source_table"      : "source_table",
        "target_db"         : "target_db",
        "target_endpoint"   : "target_endpoint",
        "target_table"      : "target_table",
        "column_key"        : "primary_key",
        "column_date"       : "created_date",
        "min_date_period"   : "2025-02-01",
        "chunk_size"        : "10000",
        "row_auto_update"   : false,
        "is_active"         : false,
        "is_autochecksum"   : false,
        "is_switched"       : false
    }
]
```

3. Execute the main script
```bash
$ python3 migration.py
```

**Notes :** Use the same approach for migration checker if needed.


# Connect with me
[![portfolio](https://img.shields.io/badge/my_portfolio-A1000F?style=for-the-badge&logo=ko-fi&logoColor=white)](https://maquebermans.github.com)
[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/mukaromalisyaban)
[![medium](https://img.shields.io/badge/medium-000?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@mukaromalisyaban)