
-- Ali Tafti; IDS 521 Fall 2014

DROP DATABASE IF EXISTS `TwitterResearch_Fall2014`;

CREATE DATABASE `TwitterResearch_Fall2014`;


-- Table without the proper indexes; useful for class lesson
-- Also, it's good to store all of the initial data without worrying about
-- key constraints .  
Create Table EarnRelDate (

	ticker VARCHAR(8),
	symbol VARCHAR(8),
	cname VARCHAR(40),
	pends DATE,
	pdicity VARCHAR(8),
	anndats DATE,
	anntimes decimal,
	actualdate DATE,
	earnrelease_date DATE,
	earnrelease_time TIME
);

-- Load the data from CSV. 
-- Use a path that the MySQL engine has permissions to access; This example was done on a Mac. Remember MS Windows systems have backslash in folder paths.
-- You may need to grant proper permissions to your user. See: http://dev.mysql.com/doc/refman/5.1/en/grant.html
LOAD DATA INFILE 'D:/earnings_processed_Sept30_2014_1110pm.csv'  INTO TABLE  EarnRelDate
CHARACTER SET utf8
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r'
IGNORE 1 LINES
(ticker, symbol, cname, pends, pdicity, anndats, anntimes, actualdate,  earnrelease_date,  earnrelease_time);

Select * from EarnRelDate where symbol is not null and symbol != "" order by earnrelease_date, earnrelease_time;
 
select count(*) from Tweets;

-- Drop table EarnRelDate2;

-- This table has primary key constraints; enforces no duplicates.
Create Table EarnRelDate2 (

	ticker VARCHAR(8),
	symbol VARCHAR(8),
	cname VARCHAR(40),
	pends DATE,
	pdicity VARCHAR(8),
	anndats DATE,
	anntimes decimal,
	actualdate DATE,
	earnrelease_date DATE,
	earnrelease_time TIME,
	PRIMARY KEY (ticker, earnrelease_date,earnrelease_time, pdicity) 	
) ENGINE=InnoDB;

-- Populate the EarnRelDate2 with distinct values
Insert into EarnRelDate2 (ticker, earnrelease_date,earnrelease_time, pdicity )
Select distinct ticker, earnrelease_date,earnrelease_time, pdicity from EarnRelDate;

Select count(*) from EarnRelDate2;
-- 40504 count from the new table after removing duplicates
Select count(*) from EarnRelDate;
-- 41581 count from the original table 


-- This table contains only the essential data needed for matching Twitter feeds.accessible
-- InnoDB is the default in MySQL 5.7: http://dev.mysql.com/doc/refman/5.7/en/innodb-introduction.html
-- All InnoDB indexes are B-trees (not Hash indexes), important because we will rely on range lookups: http://dev.mysql.com/doc/refman/5.5/en/index-btree-hash.html
Create Table EarnRelDate3 (

	ticker VARCHAR(8),
	symbol VARCHAR(8),
	earnrelease_date DATE,
	earnrelease_time TIME,
	PRIMARY KEY (ticker, earnrelease_date,earnrelease_time) 	
) ENGINE=InnoDB;


-- Populate the EarnRelDate2 with distinct values
Insert into EarnRelDate3 (ticker, earnrelease_date,earnrelease_time )
Select distinct ticker, earnrelease_date,earnrelease_time from EarnRelDate;

Select count(*) from EarnRelDate3;
-- 34174 count from the EarnRelDate3 which removes duplicate earnings events ignoring periodicity


-- drop table Tweets_test;
drop table Tweets;
-- Non-indexed table
Create Table Tweets (

	smblid VARCHAR(10),
	symbol VARCHAR(8),
	periodnum INT,
	periodnum_inday INT, 
	volumestart INT,
	volumeend INT,
	twittermentions INT,
	twitterpermin DECIMAL(10,2), 
	averagefollowers DECIMAL(10,2),
	datestart DATE,
	timestart TIME,
	dateend DATE, 
	timeend TIME	
);


-- Delete all rows from my Tweets_test table.
-- delete from Tweets_test;

-- Currently, the load is set to ignore twittermentions, twitterpersec, ... averagefollowers
-- This load command is not able to import those columns
-- You may either fix the .csv file or update the code here to handle those issues
LOAD DATA INFILE 'D:/TwitterYahoo_INTERM1_Oct4_2013.csv'  INTO TABLE  Tweets
CHARACTER SET utf8
FIELDS TERMINATED BY ','
 OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(smblid, symbol, periodnum, periodnum_inday, @volumestart, @volumeend, @twittermentions, @twitterpersec, @averagefollowers, datestart, timestart,dateend, timeend)
SET volumestart = IF(@volumestart='',0,@volumestart),
volumeend = IF(@volumeend='',0,@volumeend),
twittermentions = IF(@twittermentions='',null,@twittermentions),
twitterpermin = IF(@twitterpersec='',null,60*@twitterpersec),
averagefollowers = IF(@averagefollowers='',null,@averagefollowers);

select count(*) from Tweets;
select max(twitterpermin) from Tweets;
select avg(averagefollowers) from Tweets;
select count(*) from Tweets where smblid is not null;

-- This table has indexes and a primary key constraint. 
Create Table Tweets2 (
	smblid VARCHAR(10),
	periodnum INT,
	periodnum_inday INT, 
	datestart DATE,
	timestart TIME,
	dateend DATE, 
	timeend TIME,	
	PRIMARY KEY (smblid, periodnum), 	
	INDEX tstart (smblid, datestart, timestart),
	INDEX tend (smblid, dateend, timeend)
);

-- Populate the Tweets2 with distinct values
Insert into Tweets2 (smblid,periodnum, periodnum_inday, datestart, timestart, dateend, timeend) 
Select distinct smblid,periodnum, periodnum_inday, datestart, timestart, dateend, timeend from Tweets;

select count(*) from Tweets2;



-- Run this with and without explain
explain Select e.ticker, e.earnrelease_date, e.earnrelease_time,
t.smblid, t.periodnum, t.periodnum_inday, t.datestart, t.timestart, t.dateend, t.timeend 
 from Tweets2 t, EarnRelDate3 e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;

Select count(*)
 from Tweets2 t, EarnRelDate3 e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;


Create Table EarnRelMatched (
	ticker VARCHAR(10),
	earnrelease_date DATE,
	earnrelease_time TIME,
	smblid VARCHAR(10),
	periodnum INT, 
	periodnum_inday INT,
	datestart DATE, 
	timestart TIME,
	dateend DATE, 
	timeend TIME,
	PRIMARY KEY (smblid, periodnum) 	
);


-- Query will be slow without the proper indexes.
-- Don't bother running without 'explain', or be prepared to wait or 
-- terminate. 
explain Select e.ticker, e.earnrelease_date, e.earnrelease_time,
t.smblid, t.periodnum, t.periodnum_inday, t.timestart, t.timeend 
 from Tweets t, EarnRelDate e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid; 


-- Query will be slow without the proper indexes!
explain Select count(*)
 from Tweets t, EarnRelDate e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;


-- Load into EarnRelMatched based on results of the query
INSERT into EarnRelMatched
Select e.ticker, e.earnrelease_date, e.earnrelease_time,
t.smblid, t.periodnum, t.periodnum_inday, t.datestart, t.timestart, t.dateend, t.timeend 
 from Tweets2 t, EarnRelDate3 e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;

Select count(*) from EarnRelMatched;