# Data Pipeline

The data pipeline is for C-Tran, a transit organization for Clark County in the State of Washington, GPS sensor data of its buses and bus stops that the buses serve.
The pipeline automatically and periodically gathers the GPS sensor  and bus stops data from web servers. 

Once data is gathered, the data pipeline parses it into individual breadcrumb readings, transports the data through an asynchronous event system (Kafka). 

Kafta sends the data to validation for testing and data transformation the format of the data that is needed for analysis. 

After that, the validator sends data to be loaded to a SQL database.

Once data is in the SQL database, it could be associated with each other (GPS sensor with stops/route data) so it can be queried by route (stops) as well as geographic region and time of day.

The basic architecture of the data pipeline:


![Screen Shot 2021-06-06 at 8 05 11 PM](https://user-images.githubusercontent.com/52648061/120954882-651f8980-c70d-11eb-86f1-8fa84ba50d6b.png)
