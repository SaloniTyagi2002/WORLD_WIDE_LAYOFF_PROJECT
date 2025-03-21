SELECT *
FROM layoffs;

CREATE TABLE layoff_staging
LIKE layoffs;

INSERT layoff_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoff_staging;

-- 1 removing duplicates as first step in data cleaning

SELECT *,
ROW_NUMBER() OVER (PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num
FROM layoff_staging;

WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,country,funds_raised) AS row_num
FROM layoff_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoff_staging
WHERE company='Oda';

-- CREATE TABLE layoff_staging2
-- LIKE layoff_staging;

-- SELECT *
-- FROM layoff_staging2;

-- INSERT layoff_staging2
-- SELECT *
-- FROM layoff_staging;

-- SELECT *
-- FROM layoff_staging2;

CREATE TABLE `layoff_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoff_staging3
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,country,funds_raised) AS row_num
FROM layoff_staging;

SELECT *
FROM layoff_staging3;

SELECT *
FROM layoff_staging3
WHERE row_num>1;

-- to toggle safe mode as OFF in sql for delete/update statement
SET SQL_SAFE_UPDATES=0;    

DELETE 
FROM layoff_staging3
WHERE row_num>1;

SELECT *
FROM layoff_staging3
WHERE row_num>1;

-- 2 standardizing data

SELECT distinct(company)
FROM layoff_staging3;

SELECT company,TRIM(company)
FROM layoff_staging3;

UPDATE layoff_staging3
SET company=TRIM(company);

SELECT DISTINCT industry
FROM layoff_staging3
ORDER BY 1;

SELECT *
FROM layoff_staging3
WHERE industry LIKE 'Crypto%';

UPDATE layoff_staging3
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';



SELECT DISTINCT location
FROM layoff_staging3
ORDER BY 1;

select distinct country
FROM layoff_staging3
order by 1;

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoff_staging3;

ALTER TABLE layoff_staging3 
MODIFY COLUMN `date` DATE;
 
 
 -- NO MISSING VALUE AND NULL VALUES ARE THERE IN DATASET.
 
 



SELECT COLUMN_NAME, EXTRA 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'layoff_staging3' 
AND COLUMN_NAME = 'row_num';

ALTER TABLE layoff_staging3 DROP COLUMN row_num;

SELECT *
FROM layoff_staging3;

SELECT MAX(total_laid_off)
FROM layoff_staging3;

SELECT *
FROM layoff_staging3
ORDER BY total_laid_off DESC;

SELECT *
FROM layoff_staging3
ORDER BY funds_raised DESC;

SELECT company, SUM(total_laid_off)
FROM layoff_staging3
GROUP BY company
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off)
FROM layoff_staging3
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoff_staging3
GROUP BY country
ORDER BY 2 DESC;

SELECT MIN(`date`) AS minimum, MAX(`date`) AS maximum
FROM layoff_staging3;

SELECT YEAR(`date`) AS yearly_layoff, SUM(total_laid_off)
FROM layoff_staging3
GROUP BY YEAR(`date`)
ORDER BY 1;

SELECT SUBSTRING(`date`,1, 7) AS `MONTH`,SUM(total_laid_off)
FROM layoff_staging3
WHERE SUBSTRING(`date`,1, 7) IS NOT NULL
GROUP BY SUBSTRING(`date`,1, 7)
order by 1 ASC;

WITH Rolling_Total AS(
	SELECT SUBSTRING(`date`,1, 7) AS `MONTH`,SUM(total_laid_off) AS total_off
	FROM layoff_staging3
	WHERE SUBSTRING(`date`,1, 7) IS NOT NULL
	GROUP BY SUBSTRING(`date`,1, 7)
	order by 1 ASC
)
SELECT `MONTH`,total_off,SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoff_staging3
GROUP BY company,YEAR(`date`)
ORDER BY  1 ASC;

SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoff_staging3
GROUP BY company,YEAR(`date`)
ORDER BY  3 DESC;



SELECT VERSION();

SHOW COLUMNS FROM Rolling_Total;



WITH company_year AS (
    SELECT company, 
           YEAR(`date`) AS years, 
           SUM(total_laid_off) AS total_laid_off  
    FROM layoff_staging3  
    GROUP BY company, YEAR(`date`)
),
company_year_rank AS (
    SELECT *, 
           DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM company_year
)
SELECT * 
FROM company_year_rank
WHERE ranking>=5;






















