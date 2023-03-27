# GCP_ETL_Data_transfer
Project Introduction:
This project mainly applies the application side of the BigQuery to Spanner data flow in the GCP service. The purpose is to effectively transfer BQ data to Spnnaer.
The main advantage is that because of the large data throughput, there are self-written Multi-Thread tools for parallel processing and accelerated operation.

user's guidance:
It can be applied to main.py to add different parameters and import them into different services, and quickly summarize the corresponding required data content (because the situation I encountered will have data applications from different platform providers, so different scenarios need to be added).

Code example:
For example, in the main program, argv = ["-h", f"{hall}", "-b", f"{begin_date}", "-e", f"{end_date}"]
Different variables can be added to transfer the required time and date data from BQ to Spanner
