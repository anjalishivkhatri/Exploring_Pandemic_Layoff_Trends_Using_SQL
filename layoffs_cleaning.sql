##...........Data Cleaning.......


USE world_layoffs;

SELECT *
FROM layoffs;

##....Step 1: Remove Duplicates........
##....Step 2: Standardize the data......
##....Step 3: Null Values or blank values....
##....Step 4: Remove any columns or rows which are not useful

##...Note for step 4: In real life scenarios, there are ETL processes which import data from
## different data sources and filter the data without removing any unnecessary rows or columns
## At workplace, it's important to remember that you will likely not
## remove any columns from the dataset as it can create problems

## To make our analysis easier while keeping the raw data
## we can create a staging table 

CREATE TABLE layoffs_staging
LIKE layoffs;
## Creating a staging layoff table 

SELECT *
FROM layoffs_staging;
## Columns have been created in the staging table, now we need to insert data

INSERT layoffs_staging
SELECT *
FROM layoffs; 

## We're going to change staging database a lot and in case we make mistakes
## we will have the raw dataset available 


##.....Step 1: Removing Duplicates........

## Since the rows in our dataset has no unique identifier, it will be a more 
## difficult process 

SELECT *,
ROW_NUMBER() OVER( 
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

## Row number partitioned by columns, partition by clause is used to identify the columns on which records
## are duplicated on 
## The resulting row numbers are mostly unique, we want to be able to filter on this
## for example, filtering on row number greater than 2 which indicates duplicates 

## Creating a subquery or CTE to remove the duplicates 

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER( PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

## The result returns a couple of rows which are duplicates or are present in the
## dataset twice. 
## Remember we want to eliminate only one record of the duplicate and not both

## One way to delete these duplicate records is through DELETE statement within CTE
## But that may not be applicable depending on SQL or MYSQL application we're using

## Creating another staging table that has the extra rows and then 
## deleting it where that row is equal to 2 

## Another method to create table without writing the whole query
## Right click on the table >> copy to clipboard >> Create statement 
## Now rename the table name and assign the data type 

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

## Now we have an empty table layoffs staging 2 and now we can insert 
## data into it 

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off,`date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging; 

## Now that we have inserted the copy of data in staging 2 table,
## we can go ahead and delete the duplicates 

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SET sql_safe_updates = 0;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

## Recap of removing duplicates
## a) We established that our dataset had no unique identifier for rows
## b) We used ROW_NUMBER and PARTITION BY functions to match the row num against each column
## which helped us identify the duplicates by assigning row num = 2 for any record that was a duplicate 
## c) We created a CTE/subquery to give us duplicates 
## d) Then we created a staging layoffs 2 table to store the data and delete duplicates

SELECT *
FROM layoffs_staging2;


##.............Standardizing Data.............

## Finding issues in your data and then fixing them

SELECT DISTINCT company
FROM layoffs_staging2;

SELECT DISTINCT(TRIM(company))
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

SET sql_safe_updates = 0;

UPDATE layoffs_staging2
SET company = TRIM(company); 

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

## Returned results show there are some blank rows as well as
## multiple rows for the same category like crypto, crypto currency, cryptocurrency
## which need to be grouped together for accurate analysis 

## We'll start by updating crypto

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

## a few values in country column have a full stop making them display
## as 2 separate rows

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

## We can remove the dot with TRIM and TRAILING statements

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT *
FROM layoffs_staging2;

## We can see that the date column has the text data type
## To do a time series analysis, we will have to change the data type of Date

SELECT `date`
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

## Now the dates have been formatted but the date column is still
## in the text format 

## Never use this on your raw data 
ALTER TABLE layoffs_staging2 
MODIFY COLUMN `date` DATE;

## Now that we have standardized our data a bit, we will fix null values 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

## We can see that total_laid_off and percentage laid have quite
## a few null values
## if there are nulls in combination of both columns then it's not very useful

SELECT *
FROM layoffs_staging2
WHERE industry is NULL
OR industry = '';

## We will verify if any of the records have industry populated for the 
## company shown in our results, for example checking if any of the Airbnb 
## record have industry populated

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';
## shows that one of the records for Airbnb has "Travel" populated in the industry so
## we know it belongs to the Travel industry and we can populate that value
## in the record with blank cell

## To do this we will use a JOIN statement on itself
## We will check if in the table does it have a blank cell and not a blank cell
## If a blank, update it with the value of non blank 

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
	AND t1.location = t2.location  
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

## The above statement gives us the records where there are records with
## either blank industry cells or where the values are null for a category of industry

## To make it more clear, we can run a query specifying only the industry columns

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

## Now we will update the blank industry values

SET sql_safe_updates = 0;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; 
	
    
## The returned results show that the values have not been updated
## This could be because these values are blank and not actually nulls

## Sometimes we have to set the blank values to Null in order to replace those

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

## Now if we run the same query, we will see the blank values have been replaced by
## the null values 

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
    
## Now we can see that the row with null industry values have been updated     
SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb'; 

SELECT *
FROM layoffs_staging2
WHERE industry is NULL
OR industry = '';   

## Now Bally's is the only company where industry is null and that's because there is
## only one record of this company 

## There are still some columns with null values like total laid off and percentage laid off etc.
## but we will leave those columns as is as we may have to scrape the web to find out
## these data points 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


## We can see that some rows have null values for both the columns total_laid_off
## and percentage_laid_off 
## While deleting data can be tricky and depends on a lot of factors, 
## in our case we will get rid of these rows to make our analysis more acourate
## Since we can't trust the rows where none of those values have been provided/captured

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

## We can drop column Row Num since it's not going to be useful anymore
## and will take up additional space 

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


##...........Data is now ready for exploratory analysis................