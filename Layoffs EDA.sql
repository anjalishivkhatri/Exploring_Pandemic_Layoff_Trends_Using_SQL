##..............Continuing Exploratory Data Analysis on a Clean Layoffs Staging data........

USE world_layoffs;

SELECT * FROM layoffs_staging2;

## 1) What was the max number of people who got laid off in a day 

SELECT 
MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

#The highest number of people who got laid off in one day is 12000
# 1 represents percentage indicating 100% of the company was laid off 

##2) List the companies who had 100% of the employees laid off sorted by the highest number of total
## employee laid off 

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

## Katerra company laid off the highest number of employees, 2434 in total which 
## is 100% of it's workforce 

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

##3) List the companies by the total number of employees they have laid off till the day 

SELECT company,SUM(total_laid_off) as total_emp_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_emp_laid_off DESC;

# Amazon has let go the highest number of people at 18150 

##4) List the number of employees laid off by the date
SELECT company,SUM(total_laid_off) as total_emp_laid_off, `date`
FROM layoffs_staging2
GROUP BY company, `date`
ORDER BY total_emp_laid_off DESC;

##5) What's the most recent and oldest date for the layoff dataset 

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

## The dates in our dataset ranges between 03-2020 to 03-2023 
## The layoffs may have started around the time Covid hit the United States in early 2020

##6) Which industry was impacted the worst in terms of layoffs

SELECT industry, SUM(total_laid_off) as total_by_ind
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_by_ind DESC;

## Consumer and Retail industries were impacted the worst with layoffs # of 45182 and 43613 Respectively 
## Manufacturing and Fin-tech industries were the least impacted industries 

##7) Which country had the highest number of layoffs

SELECT country, SUM(total_laid_off) as total_by_country
FROM layoffs_staging2
GROUP BY country
ORDER BY total_by_country DESC;

## United states had the highest number of layoffs at 256559
## Followed by India at 35993 

##8) List the total number of layoffs by Year

SELECT YEAR(`date`), SUM(total_laid_off) AS Total_By_Year
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY Total_By_Year DESC;

## Year of 2022 seems to be the worst year for layoffs but since we have only 3 months of data
## for the year of 2023, the number could be higher for 2023 in comparison to year 2022 

##9) List the stage of the companies and list the total of companies in each category of the stage

SELECT stage, SUM(total_laid_off) AS companies_by_stage
FROM layoffs_staging2
GROUP BY stage
ORDER BY companies_by_stage DESC;

## majority of the companies are in Post-IPO stage at 204132 

##Now we will look at the progression of the layoffs. Starting from the earliest of the layoffs 
## and then the rolling sum till the end of the layoffs

##10) Rolling sum by the month 

SELECT SUBSTRING(`date`, 6,2) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `MONTH`;

## In the above query we're pulling the month from date column, at 6th position, pulling 2 digits 

## Even though we're getting the rolling total by the month, we're not getting 
## the complete information as the results are not grouped by month and the year 

SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

## This will give us all the results grouped by each month of the year from 2020 to 2023

## Now we will get the rolling sum based on the results we got from the previous query

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS Total_Off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, Total_Off
,SUM(Total_Off) OVER(ORDER BY `MONTH`) AS Rolling_Total
FROM Rolling_Total;

## Subquery gives us the sum of employees laid off by each month of the year
## Outer query is based on the results of the inner query 
## In outer query, we are selecting the month, sum of the column total_laid_off and ordering by month
## we're not using Partition by with OVER since we have already grouped the data by month in inner query
## Now we will have the sum by each month plus the rolling total for each month 

## From the results it looks like the number of layoffs significantly in 2022 

## 11) How many employees did companies lay off each year 

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;