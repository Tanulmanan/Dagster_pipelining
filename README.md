DAGSTER

Dagster is an orchestrator that's designed for developing and maintaining data assets, such as tables, data sets, machine learning models, and reports.

Dagster supports Python 3.7+.
DAGSTER does not have auto reload, so after every major change change start the server again, it can handle minor changes in the functions.

INSTALLATION:
To install the latest stable version of the core Dagster packages in your current Python environment, run:

For windows:

•	pip install dagster dagit


NOTE:
While installing dagit it will require urlib3 python library but its version should be below 2.0.0 and default urlib3 in all python environments is 2.0.3.

To solve this:
•	pip uninstall urlib3
•	pip install urlib3==1.26.16


For MAC with M1 or M2 chip:

•	pip install dagster dagit --find-links=https://github.com/dagster-io/build-grpcio/wiki/Wheels



CREATING A NEW PROJECT:
1.	 Bootstrap a new project
o	dagster project scaffold --name my-dagster-project


2.	 Install project dependencies
o	pip install -e ".[dev]"

3.	Start the Dagster UI
o	dagster dev


This project has separate files defined for each component:
•	All the transformations and extra functions are defined in an ops.py file.
•	All the assets are defined in the assets folder with separate file being defined for each type of data.
•	All the jobs are defined in jobs.py file.
•	All the schedules are defined in schedules.py file.
•	The project also has a file called s3.py which has all the S3 functions defined in it.


COMPONENTS OF A DAGSTER PROJECT:

1.	ASSETS:

Assets represent the data outputs produced by solids within a pipeline. An asset is a unit of data that is valuable and meaningful within the context of your data workflows. It can be a file, a database table, a dataset, or any other data artifact that you consider important for your analysis or downstream processes.
Assets play a crucial role in managing data lineage, quality, and observability in Dagster. By treating data as assets, Dagster helps you track and document the flow of data through your pipelines, enabling better data governance and understanding.
Here are some key aspects of assets in Dagster:
1.	Definition: Assets are defined within the context of solids. Each solid can produce one or more assets as outputs. For example, if a solid performs an ETL operation and generates a CSV file and a database table, both of these can be defined as assets.
2.	Metadata: Assets can have associated metadata, such as descriptions, tags, sources, and schemas. This metadata helps provide additional context and documentation about the assets, making it easier to understand their purpose and characteristics.
3.	Lineage: Dagster tracks the lineage of assets, which means it captures the relationships between inputs and outputs of solids. This allows you to trace the origin of an asset and understand which solids and pipelines have contributed to its creation.
4.	Quality checks: Dagster provides functionality to perform quality checks on assets. You can define validation and verification checks to ensure the correctness, integrity, and compliance of your data. These checks can be performed at different stages of the pipeline, helping to maintain data quality throughout the workflow.
5.	Asset catalog: Dagster includes an asset catalog that serves as a central repository for managing and browsing assets. The catalog provides a unified view of all the assets in your pipelines, along with their metadata and lineage information.

•	Assets need to be imported from the dagster module and in the assets.py file the decorator- ‘@assets’ needs to used on any function and dagster understands that it is an asset.

•	To create an asset that it dependent on the above one the first asset needs to be passed as a parameter in the second asset.

•	Metadata can be passed in the assets to show graphs and numbers like the example above.

2.	SCHEDULING:

A job lets you target a selection of assets to materialize them together as a single action. Assets can belong to multiple jobs.
Dagster's AssetSelection module lets you choose which assets to attach to a job. In the example above, AssetSelection.all selects all assets.
Once you have a job, you can execute it on a schedule, by clicking a button in the UI, the CLI, or via Dagster's GraphQL endpoints.


•	‘cron_schedule’ is the parameter that helps in scheduling jobs.

•	@Schedule Decorator that defines a schedule that executes according to a given cron schedule.




Some examples of cron_schedule:

1. cron_schedule="0 * * * *",

The cron_schedule you provided, "0 * * * *", represents a cron schedule expression that executes a task every hour at the beginning of the hour.
To break down the cron schedule expression:
•	The first field represents minutes, and "0" means the task will run at the 0th minute of each hour.
•	The second field represents hours, and the "*" wildcard means the task will run for any hour.
•	The third field represents days of the month, and the "*" wildcard means the task will run for any day.
•	The fourth field represents months, and the "*" wildcard means the task will run for any month.
•	The fifth field represents days of the week, and the "*" wildcard means the task will run for any day of the week.
By using this cron schedule expression, your task will execute every hour at the beginning of the hour, regardless of the specific day or month.
2. cron_schedule = "0 8 * * *"

In this example, the cron schedule expression "0 8 * * *" represents the following:
•	The first field represents minutes and is set to "0", indicating that the task will run at the 0th minute of the specified hour.
•	The second field represents hours and is set to "8", indicating that the task will run at 8 AM.
•	The third field represents days of the month and is set to "*", indicating that the task will run on any day.
•	The fourth field represents months and is set to "*", indicating that the task will run in any month.
•	The fifth field represents days of the week and is set to "*", indicating that the task will run on any day of the week.
By using this cron schedule expression, the task will execute every day at 8 AM. Adjust the hour value as needed to match the desired time for your task to run.

Basic Schedules:
Here's a simple schedule that runs a job every day, at midnight:

Example:

@job
def my_job():
    ...


basic_schedule = ScheduleDefinition(job=my_job, cron_schedule="0 0 * * *")
The cron_schedule argument accepts standard cron expressions. It also accepts "@hourly", "@daily", "@weekly", and "@monthly" if your croniter dependency's version is >= 1.0.12.


Schedules from partitioned assets and jobs:

When you have a partitioned job that's partitioned by time, you can use the build_schedule_from_partitioned_job function to construct a schedule for it whose interval matches the spacing of partitions in your job.
For example, if you have a daily partitioned job that fills in a date partition of a table each time it runs, you likely want to run that job every day.
The Partitioned Jobs concepts page includes an example of how to define a date-partitioned job. Having defined that job, you can construct a schedule for it using build_schedule_from_partitioned_job. For example:
from dagster import build_schedule_from_partitioned_job, job


Example:

@job(config=my_partitioned_config)
def do_stuff_partitioned():
    ...


do_stuff_partitioned_schedule = build_schedule_from_partitioned_job(
    do_stuff_partitioned,
)



Note:
Remember to consider the time zone in which your system or task scheduler is configured. The cron schedule expression provided assumes the time zone set on the system running the scheduler.

Once the dagster project has been created, the assets need to be defined in an assets.py file and dagster acceses the __init__.py file for the definitions.
If any of the components like assets, jobs schedules have not been defined in the definitions function then the dagster will not be able to access that component and it will show an error.

3.	Partitions:

A software-defined asset can represent a collection of partitions that can be tracked and materialized independently. In many ways, each partition functions like its own mini-asset, but they all share a common materialization function and dependencies. Typically, each partition will correspond to a separate file, or a slice of a table in a database.
A common use is for each partition to represent all the records in a data set that fall within a particular time window, e.g. hourly, daily or monthly. Alternatively, each partition can represent a region, a customer, an experiment - any dimension along which you want to be able to materialize and monitor independently. An asset can also be partitioned along multiple dimensions, e.g. by region and by hour.
A graph of assets with the same partitions implicitly forms a partitioned data pipeline, and you can launch a run that selects multiple assets and materializes the same partition in each asset.
Similarly, a partitioned job is a job where each run corresponds to a partition. It's common to construct a partitioned job that materializes a single partition across a set of partitioned assets every time it runs.


In this project 2 types of partitions are used:
Monthly and Daily Partitions.

All partitions need to be defined as a parameter of the asset decorator.
Example:

@asset(partitions_def=DailyPartitionsDefinition(start_date="2022-01-01"))

The above partitions used are time partitions and the parttions are created from the start date till the current date.

Dagster can handle different partitions being defined on different assets in a lineage.

•	A Lineage is DAG which shows the flow between the assets.

•	Partitions can be run from the Dagster UI  and from the CLI.
•	A partition can also be run individually, in a range of date or all can be run at once.
•	Dagster executes the partitions parallelly.
•	Partitions can be configured from the UI with the help of the launchpad.



Using the Backfill CLI

You can also launch backfills using the backfill CLI.
In the Partitions  we can define a partitioned job called do_stuff_partitioned that has date partitions. 

•	Having done so, we can run the command dagster job backfill to execute the backfill.

$ dagster job backfill -p do_stuff_partitioned

This will display a list of all the partitions in the job, ask you if you want to proceed, and then launch a run for each partition.

Executing a subset of partitions#
You can also execute subsets of the partition sets.

•	You can specify the --partitions argument and provide a comma-separated list of partition names you want to backfill:

$ dagster job backfill -p do_stuff_partitioned --partitions 2021-04-01,2021-04-02

•	Alternatively, you can also specify ranges of partitions using the --from and --to arguments:

$ dagster job backfill -p do_stuff_partitioned --from 2021-04-01 --to 2021-05-01

4.	JOBS:

Jobs are the main unit of execution and monitoring in Dagster.
A job does one of two things:
•	Materializes a selection of Software-defined Assets
•	Executes a graph of ops, which are not tied to software-defined assets

Jobs can be launched in a few different ways:
•	Manually from the UI
•	At fixed intervals, by schedules
•	When external changes occur, using sensors

From software-defined assets:

It is done by using the function: define_asset_job

Example:

partitioned_asset_job_ercot_dam = define_asset_job(
    "ercot_dam_job",
    selectilon=AssetSelection.assets(process_ercot_dam, split_and_upload_ercot_dam),
    partitions_def= DailyPartitionsDefinition(start_date="2022-01-01")
)

Directly from ops:

The simplest way to create an op-based job is to use the job decorator.

Example:

from dagster import job, op

@op
def return_five():
    return 5
@op
def add_one(arg):
    return arg + 1

@job
def do_stuff():
    add_one(return_five())

For more information:

https://docs.dagster.io/concepts/ops-jobs-graphs/jobs

https://docs.dagster.io/concepts/ops-jobs-graphs/job-execution


Using the dagster cli:

Dagster asset:

Commands for working with Dagster assets:

dagster asset [OPTIONS] COMMAND [ARGS]...
Commands:

•	list
o	List assets
•	materialize
o	Execute a run to materialize a selection…
•	wipe
o	Eliminate asset key indexes from event logs.
•	wipe-partitions-status-cache
o	Clears the asset partitions status cache,…


dagster dev:

Start a local deployment of Dagster, including dagit running on localhost and the dagster-daemon running in the background

dagster dev [OPTIONS]
Options:

•	-d, --working-directory <working_directory>¶
o	Specify working directory to use when loading the repository or job
•	-m, --module-name <module_name>¶
o	Specify module or modules (flag can be used multiple times) where dagster definitions reside as top-level symbols/variables and load each module as a code location in the current python environment.
•	-f, --python-file <python_file>¶
o	Specify python file or files (flag can be used multiple times) where dagster definitions reside as top-level symbols/variables and load each file as a code location in the current python environment.
•	-w, --workspace <workspace>¶
o	Path to workspace file. Argument can be provided multiple times.
•	--code-server-log-level <code_server_log_level>¶
o	Set the log level for code servers spun up by dagster services.
•	Default:
o	warning
•	Options:
o	critical | error | warning | info | debug
•	-p, --dagit-port <dagit_port>¶
o	Port to use for the Dagit UI.
•	-h, --dagit-host <dagit_host>¶
o	Host to use for the Dagit UI.

Environment variables:

•	DAGSTER_WORKING_DIRECTORY
o	Provide a default for --working-directory
•	DAGSTER_MODULE_NAME
o	Provide a default for --module-name
•	DAGSTER_PYTHON_FILE
o	Provide a default for --python-file

Dagster job:

Commands for working with Dagster jobs.

dagster job [OPTIONS] COMMAND [ARGS]...

Commands:

•	backfill
o	Backfill a partitioned job.
•	execute
o	Execute a job.
•	launch
o	Launch a job using the run launcher…
•	list
o	List the jobs in a repository.
•	list_versions
o	Display the freshness of memoized results…
•	print
o	Print a job.
•	scaffold_config
o	Scaffold the config for a job.
•	dagster run
o	Commands for working with Dagster job runs.
•	dagster run [OPTIONS] COMMAND [ARGS]...
o	Commands
•	delete
o	Delete a run by id and its associated…
•	list
o	List the runs in the current Dagster…
•	migrate-repository
o	Migrate the run history for a job from a…
•	wipe
o	Eliminate all run history and event logs.

Dagster schedule:

Commands for working with Dagster schedules.

dagster schedule [OPTIONS] COMMAND [ARGS]...

Commands:

•	debug
o	Debug information about the scheduler.
•	list
o	List all schedules that correspond to a…
•	logs
o	Get logs for a schedule.
•	preview
o	Preview changes that will be performed by…
•	restart
o	Restart a running schedule.
•	start
o	Start an existing schedule.
•	stop
o	Stop an existing schedule.
•	wipe
o	Delete the schedule history and turn off…


For more information:

https://docs.dagster.io/_apidocs/cli#dagster-job


Multiple partitioned assets exist in assets job
