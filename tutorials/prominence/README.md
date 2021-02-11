# EasyVVUQ examples using PROMINENCE

PROMINENCE allows users to run batch jobs across multiple clouds as though they formed a single batch system. Note that the examples here have very short
running jobs and so are not appropriate for running on PROMINENCE in this way (longer running jobs are required for efficiency).

In order to run the examples firstly copy the file `cooling.template` into the `prominence` directory:
```
cp ../cooling.template .
```

It is assumed here that the EGI/EOSC PROMINENCE server is being used.
Set the environment variable `PROMINENCE_URL` to the URL of the REST API of the PROMINENCE server.
See https://prominence.readthedocs.io/en/latest/using-the-api.html.

Also ensure that you have a valid access token (see https://prominence.readthedocs.io/en/latest/command-line-interface.html#getting-an-access-token)
and ideally ensure you have a mechanism for refreshing it as needed.

## Small input and output files
If the required input files are very small (< 1MB) then they can be provided to PROMINENCE within the JSON job description. Similarly, small output files
can be obtained from the standard output from jobs.

Run the example by typing:
```
python3 easyvvuq_pce_tutorial_prominence.py
```

## Using shared storage
PROMINENCE allows volumes to be mounted in jobs from storage systems such as B2DROP or OneData. The example here makes use of B2DROP.

The file `coffee-b2drop.json` will need to be edited to include a B2DROP app username and password. This is not
the same as the username and password you use to access B2DROP. To create an app username and password, login to
https://b2drop.eudat.eu/, then select **Settings** then **Security** and click **Create new app password**.

For this example the tutorial directory will need to be copied to the shared storage and the example executed from there. Run the example by typing:
```
python3 easyvvuq_pce_tutorial_prominence_storage.py
```
