# s1-snowdepth

This package operationalizes Dr. Dunmire's machine learning model to estimate snow depth across the European Alps using Sentinel-1 backscatter. See the paper describing the model here: https://www.sciencedirect.com/science/article/pii/S003442572400395X#s0110

## Installation

This package is currently available on TestPyPi. 
First, create a virtual environment to install the package into:
`python -m venv venv && source venv/bin/activate` 

You can then install it with the following command:
`pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ s1-snowdepth`


There are a number of commands built into the package to help with setting up the necessary environments.
The pip package includes all dependencies for downloading and preprocessing the data, while the conda environment is necessary for running the model. 
In both cases, you must have a .env file containing the necessary filepaths and API keys. See the section below for more details on the .env file.

### Conda
To install the conda environment, run the command `s1-snowdepth env-create` which installs a conda environment from _environment.yml_.
You must then deactivate the virtual environment the package was installed in, activate the conda environment, and reinstall this package within conda.
- `deactivate`
- `conda activate s1-snowdepth`
- `pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ s1-snowdepth`

### Downloading static variables
One of the inputs to the model is a set of static terrain variables over the Alps. This includes elevation, slope, aspect, TPI, snowclass, and many more. 
To download this data, run the command `s1-snowdepth download-static-vars`. You can optionally set the directory these will be downloaded to with
`s1-snowdepth download-static-vars path/to/output`. The total size of these files is 842 MB.

### Downloading the model
In order to download the pre-trained model pickle file, run `s1-snowdepth download-model`. Again, a download path can optionally be passed as an argument after the command.

### Downloading sample data
If you would like to download sample data to run the model for Mar 1, 2019, the appropriate files have been provided for you and can be downloaded with the command `s1-snowdepth download-sample-data`. Once again, you can optionally specify a directory for them to be downloaded into.
These files are only about 21 MB total.

### .env
To create the .env file, `cd` into the directory you would like for it to be created in, then run `s1-snowdepth init`
which copies _.env.example_ into _.env_. Fill out the values accordingly.
OUTPUT_DIR should be created and can be wherever you would like.
If you downloaded sample data using the command above, then S1_MOSAIC_DIR, S1_SCALING_DIR, and SNOW_COVER_DIR must be set to point to the appropriate directories. Otherwise they can be whatever you would like.
All of the static variable paths should point to the location of each file downlaoded from when you ran `s1-snowdepth download-static-vars`.
MODEL_PATH should point to directory containing the final_model_xg.pkl file you downloaded beforehand, and the MODEL_NAME does not need changed.
For setting up ASF Search and Google Earth Engine credentials, see the sections below.

### Alaska Satellite Facility
This package relies on the free services provided by Alaska Satellite Facility (ASF) for downloading sentinel-1 data and processing it on their supercomputer network.
Create an account at https://search.asf.alaska.edu/#/ and store your username and password between quotes in the .env.
By default, ASF provides 10,000 free HPC credits per month which is way more than enough for anything from this model.

### Google Earth Engine
To download MODIS data, this package relies on the free services provided by Google Earth Engine.
You must create a Google Earth Engine account, followed by a cloud project that has the Earth Engine API enabled. 
See this guide on how to set that up: https://developers.google.com/earth-engine/guides/access

Once you have the project set up with the API enabled, set the project name in the .env.

Note that this method uses browser authentication. Support for an alternative method that works on headless systems is coming soon.

## Running the model
First, follow all the steps above to install the package and its environments. Ensure that you have the conda environment activated.
To run the model on all mosaic tiles located in the S1_MOSAIC_DIR, run `s1-snowdepth run`.

## Downloading new data
There are a couple of commands to download new data and do all of the preprocessing to be able to run the model.

### Sentinel-1 Mosaic
In order to download and process new sentinel-1 data for a specific date (YYYYMMDD), orbit number (OOO), and region, run
`s1-snowpepth create-s1-mosaic --year YYYYMMDD --orbit OOO --bbox min_lon min_lat max_lon max_lat`. This will download the appropriate sentinel-1 data if available, and process it on ASF's HPC platform.

### Sentinel-1 Scaling
The S1 Scaling file provides an averaged snow-free backscatter image from the same year that the model is run on. This requires significant HPC processing on all sentinel-1 images between August 1 and December 31 of the same snow year that the model is being run for.
This can take quite a lot of time to process (6-24 hours for a small region). To run this, use the command `s1-snowdepth build-scaling --orbit OOO --year YYYY --bbox min_lon min_lat max_lon max_lat`. Where the year is the snow year (2018 for a model run on Mar 1 2019).

### Snow cover
Snow cover data is generated by averaging IMS binary snowcover data with MODIS MOD10A1 gap-filled snow cover data. This requires downloading 1 day of IMS data and 5 days of MODIS data.
The command to do this is `s1-snowdepth create-snowcover YYYYMMDD` is based on the extent set by GEE_SEARCH_BBOX set in your .env.
