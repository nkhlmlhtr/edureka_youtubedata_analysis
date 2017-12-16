
-------------------Running the jar file to process the youtubedata.txt file-----------------

hadoop jar /home/cloudera/Desktop/YoutubeAnalysis.jar YoutubeAnalysis /input/youtubedata.txt /youtube_mr_output

-------------------Creating EXTERNAL table on top of mapreduce output----------------------------

CREATE EXTERNAL TABLE `db_youtube.Youtube_Data_Table`
(
`video_id` string COMMENT '',
`channel_name` string COMMENT '',
`days_interval` int COMMENT '',
`video_category` string COMMENT '',
`video_length` int COMMENT 'In Seconds',
`video_views` bigint COMMENT '',
`video_ratings` float COMMENT '',
`no_of_ratings` int COMMENT '',
`no_of_comments` int COMMENT '',
`related_video_ids` string COMMENT ''
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://quickstart.cloudera:8020/youtube_mr_output';


-------------------top 5 categories with maximum number of videos uploaded----------------------------

SELECT counts,video_category FROM
(
SELECT
RANK() OVER (ORDER BY counts DESC) AS RN,
counts,
video_category
FROM
(
SELECT COUNT(video_id) AS counts,video_category FROM db_youtube.Youtube_Data_Table GROUP BY video_category
) 
AS A
) AS B WHERE RN <= 5;

------------------top 10 rated videos----------------------------

SELECT video_id,no_of_ratings FROM(
SELECT 
video_id,
no_of_ratings,
RANK() OVER(ORDER BY no_of_ratings DESC) RN 
FROM db_youtube.Youtube_Data_Table) AS A WHERE RN <=10;

------------------most viewed videos----------------------------


SELECT video_id,video_views FROM(
SELECT 
video_id,
video_views,
RANK() OVER(ORDER BY video_views DESC) RN 
FROM db_youtube.Youtube_Data_Table) AS A WHERE RN <=10;