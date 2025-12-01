USE STADVDB;

-- 1. Reset: Drop the table if it exists to ensure a clean slate
DROP TABLE IF EXISTS movies;

-- 2. Create the Table: Single denormalized table with the 5 required columns
CREATE TABLE movies (
  id VARCHAR(12) NOT NULL,
  title VARCHAR(1024) NOT NULL,
  year SMALLINT UNSIGNED NOT NULL, -- Vital for Fragmentation (Node 2 vs 3)
  rating DECIMAL(3,1) NULL,        -- Vital for Concurrency (Update conflict target)
  genre TEXT NULL,                 -- Useful for Slicing/Filtering
  PRIMARY KEY (id),
  INDEX idx_year (year)            -- Speeds up the fragmentation queries
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. Populate with Forced Balance (Stratified Sampling)
INSERT INTO movies (id, title, year, rating, genre)
-- Get 25,000 "Old" Movies for Node 2
(SELECT 
    b.tconst, b.primaryTitle, b.startYear, r.averageRating, b.genres
 FROM title_basics b
 JOIN title_ratings r ON b.tconst = r.tconst
 WHERE b.titleType = 'movie' 
   AND b.startYear IS NOT NULL 
   AND b.startYear < 1980
 LIMIT 25000)

UNION ALL

-- Get 25,000 "New" Movies for Node 3
(SELECT 
    b.tconst, b.primaryTitle, b.startYear, r.averageRating, b.genres
 FROM title_basics b
 JOIN title_ratings r ON b.tconst = r.tconst
 WHERE b.titleType = 'movie' 
   AND b.startYear IS NOT NULL 
   AND b.startYear >= 1980
 LIMIT 25000);