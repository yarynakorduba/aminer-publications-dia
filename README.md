# DILA AMiner Publications Project
_Hello! :) We are Lisbora (SEM) and Yaryna (CS), this is our project submission for DILA exercise. We are both following 
our first master semester at TuGraz and are very excited to get to know more about the field of Data Science. This is our
very first Data Science related project, so we are aware that the solutions found for the task are not the most optimal ones.
Besides the struggles , we were very excited during the whole project and learnt a lot about data processing, cleaning and queries.
We want to also state that working on this project has motivated us to deepen our knowledge in the field of Data Science, 
and we will for sure continue to work and improve this project even after the submission._

The goal of this project is to process and train the scientific publications data from
[AMiner network](https://www.aminer.org/aminernetwork). The project consists of two general tasks:

**1) Data Integration and Cleaning**


**General description of the approach:** 

1. The built data warehouse schema can be found in `schemas/Final_DILA_Schema.png`
2. Files used from [AMiner network](https://www.aminer.org/aminernetwork):
* Aminer-Author.txt
* Aminer-Paper.txt
* Aminer-Author2Paper.txt (Supplementary File)
3. Data is parsed using pyspark into csv file format. `parse_data.ipynb`
4. The parsed data is cleaned using pyspark for consolidation and name disambiguation. The supplementary file 
   `Aminer-Author2Paper.txt` is used to perform data cleaning between the papers and authors datasets. `load_csv_into_schema.csv`
5. The following queries are performed: `load_csv_into_schema.csv`
* Compute paper count per unique affiliation
* Validate the pre-computed paper/ref counts and h indexes.



In order to run the project, you need to install pyspark using the command below:

`pip install pyspark`

1. To parse the data, you need to download the input txt files from 
[AMiner network](https://www.aminer.org/aminernetwork) and place them in `assets/` directory.

2. Then run `parse_data.ipynb` notebook. After running the notebook, the parsed csv files will be shown in the 
   `assets/parsedData/` directory.

3. To clean the data and run the queries for the first task, you need to run `load_csv_into_schema.ipynb` notebook. 

4. All the cleaned data that is needed for the second task should now be saved as CSV files in `assets/cleanedDFsData/` 
   directory. 

**Presenting Results:**

All of our results can be found in `results/Results.pdf` file.
The results form Q1.1 can be found in the `unique_authors_with_validated_cols_df` when `load_csv_into_schema.ipynb` 
has finished running.
The results from Q1.2 can be found in the `paper_count_per_affiliation_df` when `load_csv_into_schema.ipynb` has
finished running.


**2) Model Training and Evaluation**

Everything that we managed to do regarding this part can be found in `make_prediction.ipynb`.
We already did an analysis of the second task but did not manage to do a lot of progress in actually implementing 
and testing it.


P.s: During the time worked for this project, 10 kg of clementines were eaten and liters of coffee were drank. No animals were harmed! :") 