# Adastra-task

You are creating a pseudo-ETL system, which needs to be able to retrieve data from various sources and
trasmit the data to various sinks. By data, in this case, we mean short json messages with predefined
structure. Here is an example:
{"key": "A123", "value":"15.6", "ts":'2020-10-07 13:28:43.399620+02:00'}
You need to implement at least the following functionality:
• Data source is Simulation: this source will generate random data.
• Data source is File: the messages are read from an input file which contains a json array of messages.
• Data sink is Console: the consumed messages are printed to stdout.
• Data sink is PostgreSQL: the consumed messages are inserted in a database table in PostgreSQL
• Messages should be read and transmitted one by one until the source has no more messages. The
Simulation source is infinite - it should always have a new message, if asked. The File source is finite,
it ends when the whole file is read.
Make your interface as user-friendly as possible, e.g. make it fluent => https://en.wikipedia.org/wiki/Fluent_interface#Python :

ETL().source(source_args).sink(sink_args).run()
