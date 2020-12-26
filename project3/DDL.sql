select * from public.staging_events se limit 3;
/*
se_id|artist                 |auth     |firstname|gender|iteminsession|lastname|length   |level|location                                   |method|page    |registration |sessionid|song                                |status|ts                 |useragent                                                                                                                 |userid|
-----|-----------------------|---------|---------|------|-------------|--------|---------|-----|-------------------------------------------|------|--------|-------------|---------|------------------------------------|------|-------------------|--------------------------------------------------------------------------------------------------------------------------|------|
    7|A Fine Frenzy          |Logged In|Anabelle |F     |            0|Simpson |267.91138|free |Philadelphia-Camden-Wilmington, PA-NJ-DE-MD|PUT   |NextSong|1541044398796|      256|Almost Lover (Album Version)        |   200|2018-11-05 00:33:12|"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36"|69    |
   15|Nirvana                |Logged In|Aleena   |F     |            0|Kirby   |214.77832|paid |Waterloo-Cedar Falls, IA                   |PUT   |NextSong|1541022995796|      237|Serve The Servants                  |   200|2018-11-05 01:27:22|Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:31.0) Gecko/20100101 Firefox/31.0                                         |44    |
   23|Television             |Logged In|Aleena   |F     |            1|Kirby   |238.49751|paid |Waterloo-Cedar Falls, IA                   |PUT   |NextSong|1541022995796|      237|See No Evil  (Remastered LP Version)|   200|2018-11-05 01:30:56|Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:31.0) Gecko/20100101 Firefox/31.0                                         |44    |
 */
select * from public.staging_songs ss limit 3;
/*
ss_id|num_songs|artist_id         |artist_latitude|artist_longitude|artist_location|artist_name   |song_id           |title                     |duration |year|
-----|---------|------------------|---------------|----------------|---------------|--------------|------------------|--------------------------|---------|----|
   14|        1|ARBZIN01187FB362CC|        1.32026|       103.78871|27             |Paris Hilton  |SOERIDA12A6D4F8506|I Want You (Album Version)|192.28689|2006|
   78|        1|AR0IT221187B999C4D|       50.50101|         4.47684|BELGIUM        |The Weathermen|SOFJPHQ12A6D4FBA32|Let Them Come To Berlin   |246.17751|   0|
  142|        1|ARKUAXS11F4C841DEB|        38.8991|         -77.029|Washington DC  |Jazz Addixx   |SOLJVMI12AB018ABF0|Say Jazzy                 |266.52689|2007|
 */

select se.se_id, se.ts, se.song from public.staging_events se limit 3;

-- user_id, first_name, last_name, gender, level
select * from (
select DISTINCT 
se.userid, 
se.firstname first_name, 
se.lastname last_name, 
se.gender, 
se."level" 
from public.staging_events se 
limit 10);

-- https://docs.aws.amazon.com/redshift/latest/dg/c_Examples_of_INSERT_30.html
insert into users (first_name, last_name, gender, "level") (
	select distinct 
	se.firstname first_name, 
	se.lastname last_name, 
	se.gender, 
	se."level" 
	from public.staging_events se
);

select count(*) from users u ;
select * from users u limit 3;
/*
user_id|first_name|last_name|gender|level|
-------|----------|---------|------|-----|
     50|Katherine |Gay      |F     |free |
    114|Sylvie    |Cruz     |F     |free |
    178|Jacob     |Klein    |M     |paid |
 */

insert into songs (title, artist_id, "year", duration) (
	select distinct 
	ss.title, 
	ss.artist_id, 
	ss."year", 
	ss.duration 
	from public.staging_songs ss
);

select * from songs s limit 3;
/*
song_id|title                                                          |artist_id         |year|duration |
-------|---------------------------------------------------------------|------------------|----|---------|
     20|Water Into Ice                                                 |ARCGXRE11E2835E1DF|2008| 245.2371|
     84|Don?t Trust Chief Wiggum                                       |AR1JRJ61187B9B3F37|   0|437.60281|
    148|The Lower The Sun                                              |ARCDWKV1187B98C4CD|2005|216.13669|
 */

insert into artists ("name", location, latitude, longitude) (
	select distinct 
	ss.artist_name, 
	ss.artist_location , 
	ss.artist_latitude , 
	ss.artist_longitude 
	from public.staging_songs ss
);

select * from artists a limit 3;
/*
artist_id|name       |location      |latitude|longitude|
---------|-----------|--------------|--------|---------|
       16|Damero     |              |        |         |
       80|Ozgur Can  |              |        |         |
      144|Lamb Of God| Richmond, VA | 37.5407|-77.43365|
 */

insert into "time" (
	select 
	se.ts start_time, 
	extract (hour from start_time) "hour", 
	extract (day from start_time) "day", 
	extract (week from start_time) "week",
	extract (month from start_time) "month",
	extract (year from start_time) "year", 
	extract (dayofweek from start_time) "weekday"
	from public.staging_events se
);

select * from "time" t limit 3;
/*
start_time         |hour|day|week|month|year|weekday|
-------------------|----|---|----|-----|----|-------|
2018-11-05 01:27:22|   1|  5|  45|   11|2018|      1|
2018-11-05 01:54:02|   1|  5|  45|   11|2018|      1|
2018-11-05 02:30:17|   2|  5|  45|   11|2018|      1|
*/

insert into songplays (start_time, user_id, "level", song_id, artist_id, session_id, location, user_agent) (
	select se.ts start_time, se.userid, se."level", ss.song_id, ss.artist_id, se.sessionid, ss.artist_location, se.useragent 
	from staging_events se, staging_songs ss
	where se.page = 'NextSong'
	and se.song = ss.title 
	and se.artist = ss.artist_name
	and se."length" = ss.duration 
);

