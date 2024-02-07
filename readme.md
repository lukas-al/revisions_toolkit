# Revisions Toolkit
This project aims to develop a toolkit for ETL of data revisions. The toolkit will be developed in Python, using the kedro framework for replicable code and to support pipelines

## Project stages
The project will be developed in the following stages:
1. Extract and format headline GDP revisions
    1. Validate that this is what's expected with the GDP team
2. Extract amd format component-level GDP revisions
3. Develop pipeline to extract and format revisions for any ONS compatible dataset

## Project structure
1. Load the UK GDP revisions data from the ONS website
2. Format the data into a pandas dataframe
3. Structure the data, including the transformed versions

4. Subsequently construct the same for the GDP component level data
5. Subsequently, construct the same for any ONS series.

## Todo
- [x] Headline QGDP
- [x] Expenditure QGDP
- [x] Headline MGDP
- [x] Deflator QGDP
- [ ] Income QGDP
- [ ] Business Investment QGDP
- [ ] GVA QGDP
- [ ] NPISH QGDP