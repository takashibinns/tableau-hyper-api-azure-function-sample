import logging, os
from pathlib import Path
import azure.functions as func
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName, SqlType, CreateMode, TableDefinition, Inserter,NOT_NULLABLE, HyperException

#   Get config details from env variables
config = {
    'file_name'             : os.getenv("HYPER_FILENAME",       'events.hyper'),
    'db_name'               : os.getenv("HYPER_DB",             'events'),            
    'table_name'            : os.getenv("HYPER_TABLE",          'Extract'),
    'schema_name'           : os.getenv("HYPER_SCHEMA",         'Extract'),
}

#   Create some dummy data
def create_data():
    rows = [
        ['abc','def'],
        ['ghi','jkl'],
        ['mno','pqr'],
        ['stu','vwx'],
        ['y', 'z'],
    ]
    cols = [
        TableDefinition.Column("field1", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("field2", SqlType.text(), NOT_NULLABLE),
    ]
    return {
        'rows': rows,
        'columns': cols
    }

#   Use Hyper API
def create_hyper(data):

    # Define the table definition
    table_def = TableDefinition(
        table_name = TableName(config['schema_name'],config['table_name']),
        columns = data['columns']
    )

    # If the hyper file already exists, delete it
    try:
        os.remove(Path(config['file_name']))
    except OSError:
        pass

    # Define where to create the hyper file
    path_to_database = Path(config['file_name'])

    # Optional process parameters.
    process_parameters = {
        # Prevent hyper process from writing logs
        "log_config": ""
    }

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:

        # Optional connection parameters.
        connection_parameters = {"lc_time": "en_US"}

        # Creates new Hyper file "customer.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        parameters=connection_parameters) as connection:

            # Create schema
            connection.catalog.create_schema(schema=config['schema_name'])

            # Create the table definition
            logging.info("Creating the table definition")
            connection.catalog.create_table(table_definition=table_def)

            # Insert data
            logging.info("Insert telemtry data into hyper file")
            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(rows=data['rows'])
                inserter.execute()
            logging.info("The data was added to the table.")
        logging.info("The connection to the Hyper file has been closed.")
    logging.info("The Hyper process has been shut down.")

    return path_to_database

app = func.FunctionApp()
@app.schedule(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
def timer_trigger() -> None:

    logging.info('Start test function')

    # Generate some dummy data
    data = create_data()

    # Create the hyper database
    db_file_path = create_hyper(data)

    logging.info('Test function complete')
