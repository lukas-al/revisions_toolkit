# Revisions Toolkit

## Overview
This project is intended to help with both obtaining and constructing revisions series for a number of GDP components from the ONS. It's not doing anything super revolutionary.

The steps are:
1. Look to the ONS website and scrape the latest spreadsheets
2. Clean them up and extract the data
3. Construct the revisions series
4. Save the data

The project is structured as a Kedro project, which was generated using `kedro 0.19.2`. Take a look at the [Kedro documentation](https://docs.kedro.org/) to get started. This is done to make it simpler to understand and replicate - though I don't know whether that's actually the case...

## How to run the project
Make sure you've created an environment with the dependencies in `requirements.txt`. If not, you can do this by running the following command:

```miniconda3
conda create --name <env_name> 
pip install -r requirements.txt
```

Then navigate to the root file of this project, i.e. where this `README` is located, and activate the environment using the following command:

```miniconda3
conda activate <env_name>
```

and then run the following command to run the project:

```miniconda3
kedro run
```

You should then see the project running, with a bunch of logs and outputs. If you want to run a specific pipeline, you can do so by running the following command:

```miniconda3
kedro run --pipeline <pipeline_name>
```

The outputs will be saved in the `data/` directory.

## Common errors
If you're having trouble running the project, you might want to check the following:

1. Make sure you're in the right environment. You can check this by running `conda env list` and seeing which environment is active.

2. The ONS may have changed the format of their spreadsheets, which could cause the scraping to fail. There isn't an easy way to check for this, but you can look at the logs to see if and when the scraping is failing.

3. Sometimes the initial download might fail or be blocked by the ONS website. It might also be an issue with the Bank's firewall, though those issues should be resolved?

## Further information
* Project configuration (i.e. the base links to the ONS files) can be found in `conf/base/catalog.yml`. This defines the 'catalog' of data that the project will scrape and process.
* The code which downloads the data is primarily in `src/revisions_toolkit/datasets/gdp_datasets.py`. This contains the logic for scraping the data from the ONS website.
* The code which processes the data once it's been downloaded is primarily in `src/revisions_toolkit/pipelines/headline_Qgdp/nodes`. This contains the majority of the logic for cleaning and processing the data, with most of the other nodes simply copying the logic from here and applying it to different datasets.


## Notes
This project was completed more as a proof-of-concept, and there's lots of things I'd change about it looking back.

Here are some improvements to make:
* Flatten and simplify a lot of the config stuff. It's currently quite complex.
* Rationalise the pipelines - they're all logically doing the same thing but with different datasets. This could be abstracted out into a single pipeline.
* Rationalise the package structure. It could be improved to make it clearer what's going on and where the logic is.
* Save the raw data. Currently the raw data is not saved, which makes it difficult to debug if something goes wrong.
* Put some tests for the obvious break points, such as the scraping and formatting of the input spreadsheets. This would make it easier to debug and maintain.
* Some better documentation using Sphinx or similar would be good.


