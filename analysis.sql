/* CIV516 PROJECT WORK */ 
/* FIND the minimum and maximum stop sequences by trip */
DROP TABLE IF EXISTS ts2.route52_startends; 
CREATE TABLE ts2.route52_startends AS
SELECT 
  *, LEAD( ActualTime_ArrivalStop, 1 ) OVER ( PARTITION BY Date_Key, TripId ORDER BY StopSequence ) AS final_arrival_time  --Grab the end stop 
FROM 
  (
    SELECT 
      avl.*, st1.shape_dist_traveled AS departure_stop_sdt, st2.shape_dist_traveled AS arrival_stop_sdt
    FROM 
      (
        SELECT 
          avl.*, mm.max_ss, mm.min_ss 
        FROM 
          ts2.avl_data avl
          INNER JOIN 
          ( 
            SELECT 
              Date_Key, TripId, MAX( StopSequence ) AS max_ss, MIN( StopSequence ) AS min_ss 
            FROM 
              ts2.avl_data 
            WHERE 
              PatternName = '52F'
            GROUP BY 
             Date_Key, TripId
          ) mm 
          ON 
          avl.Date_Key = mm.Date_Key
          AND 
          avl.TripId = mm.TripId 
          AND 
          (avl.StopSequence = mm.max_ss OR avl.StopSequence = mm.min_ss ) 
      ) avl 
      INNER JOIN 
      may2018_gtfs.stops s1
      ON 
      CAST(avl.DepartureStopNumber AS STRING) = s1.stop_code
      INNER JOIN 
      may2018_gtfs.stops s2
      ON 
      CAST(avl.ArrivalStopNumber AS STRING) = s2.stop_code  --this might run into issues 
      INNER JOIN 
      may2018_gtfs.stop_times st1 
      ON 
      s1.stop_id = CAST(st1.stop_id AS INT64)
      AND 
      avl.TripId = CAST(st1.trip_id AS INT64)
      INNER JOIN 
      may2018_gtfs.stop_times st2
      ON 
      s2.stop_id = CAST(st2.stop_id AS INT64) 
      AND 
      avl.TripId  = CAST(st2.trip_id AS INT64) 
  ) 
;

/* CREATE A TABLE OF OPERATING SPEEDS, TO SLICE AND DICE LATER */ 
DROP TABLE IF EXISTS ts2.oneway_line_operating_speeds; 
CREATE TABLE ts2.oneway_line_operating_speeds AS 
SELECT 
  *, operating_dist_m / operating_time_s * 3.6 AS operating_speed_kph
FROM 
  ( 
      SELECT
        *, 
        CASE 
          WHEN raw_operating_time < 0 THEN TIME_DIFF( final_at_nexthour, leaving_time , SECOND ) 
          ELSE raw_operating_time 
        END as operating_time_s,
        (final_sdt - departure_stop_sdt ) * 1000 AS operating_dist_m,

      FROM 
        (
          SELECT 
            *, CAST( LEFT( ActualTime_DepartureStop , 2 ) AS INT64 ) as leaving_time_min , -- Pull minute from time string 
               CAST( SUBSTR(ActualTime_DepartureStop, 4, 2 ) AS INT64 ) as leaving_time_sec ,
               CAST( LEFT( final_arrival_time , 2 ) AS INT64 ) as final_at_min,
               CAST( SUBSTR(final_arrival_time, 4, 2 ) AS INT64 ) as final_at_sec,
               TIME(
                    5,
                    CAST( LEFT( ActualTime_DepartureStop , 2 ) AS INT64 ),
                    CAST( SUBSTR(ActualTime_DepartureStop, 4, 2 ) AS INT64 )
               ) AS leaving_time, 
               TIME(
                    5,
                    CAST( LEFT( final_arrival_time , 2 ) AS INT64 ),
                    CAST( SUBSTR(final_arrival_time, 4, 2 ) AS INT64 )
               ) AS final_at,
               TIME(
                    6,
                    CAST( LEFT( final_arrival_time , 2 ) AS INT64 ),
                    CAST( SUBSTR(final_arrival_time, 4, 2 ) AS INT64 )
               ) AS final_at_nexthour,
               TIME_DIFF(   TIME(
                                  5,
                                  CAST( LEFT( final_arrival_time , 2 ) AS INT64 ),
                                  CAST( SUBSTR(final_arrival_time, 4, 2 ) AS INT64 )
                             ), 
                             TIME(
                                  5,
                                  CAST( LEFT( ActualTime_DepartureStop , 2 ) AS INT64 ),
                                  CAST( SUBSTR(ActualTime_DepartureStop, 4, 2 ) AS INT64 )
                             ),
                             SECOND
               ) AS raw_operating_time 

          FROM 
            ts2.route52_startends
          WHERE 
            StopSequence = min_ss 
      ) 
) 
WHERE 
  operating_time_s <> 0 
;

/* SLICE AND DICE ONE WAY LINE OPERATING SPEEDS */
/* 1.0 Overall Average Operating Speed */ 
SELECT 
  avg( operating_speed_kph ) AS overall_avg_operating_speed_kph
FROM 
  ts2.oneway_line_operating_speeds
; --RESULT: 20.67 kph 

/* 2.0 Average operating speed by start and end */ 
SELECT 
  DepartureStop, final_arrival_stop, COUNT(*) AS sample_size, AVG( operating_dist_m ) AS avg_operating_dist_m , AVG( operating_speed_kph ) AS avg_operating_speed_kph , AVG( operating_time_s ) AS avg_operating_time_s
FROM 
  ts2.oneway_line_operating_speeds
GROUP BY 
  DepartureStop, final_arrival_stop	
ORDER BY 
  AVG( operating_speed_kph )
;

/* 3.0 Stop to Stop Level Analysis */ 
DROP TABLE IF EXISTS ts2.stop_to_stop_stats; 
CREATE TABLE ts2.stop_to_stop_stats AS 
SELECT 
  *, sdt_diff_m / stop2stop_tt_sec * 3.6 AS stop2stop_speed_kph 
FROM 
( 
SELECT 
  *, 
  CASE 
        WHEN raw_travel_time < 0 THEN TIME_DIFF( arrival_nexthour, leaving_time , SECOND ) 
        ELSE raw_travel_time 
  END as stop2stop_tt_sec
FROM 
  (
      SELECT  
        avl.*, st1.shape_dist_traveled AS departure_sdt, st2.shape_dist_traveled AS arrival_sdt, ( st2.shape_dist_traveled - st1.shape_dist_traveled ) * 1000 AS sdt_diff_m,
        /* CALCULATE TRAVEL TIME -- THINGS GET COMPLICATED HERE */
        TIME(
                    5,
                    CAST( LEFT( ActualTime_DepartureStop , 2 ) AS INT64 ),
                    CAST( SUBSTR(ActualTime_DepartureStop, 4, 2 ) AS INT64 )
               ) AS leaving_time,
        TIME(
                          6,
                          CAST( LEFT( ActualTime_ArrivalStop , 2 ) AS INT64 ),
                          CAST( SUBSTR(ActualTime_ArrivalStop, 4, 2 ) AS INT64 )
                    ) AS arrival_nexthour,
        TIME_DIFF(   TIME(
                          5,
                          CAST( LEFT( ActualTime_ArrivalStop	 , 2 ) AS INT64 ),
                          CAST( SUBSTR(ActualTime_ArrivalStop	, 4, 2 ) AS INT64 )
                      ), 
                      TIME(
                          5,
                          CAST( LEFT( ActualTime_DepartureStop , 2 ) AS INT64 ),
                          CAST( SUBSTR(ActualTime_DepartureStop, 4, 2 ) AS INT64 )
                      ),
                      SECOND
        ) AS raw_travel_time 
      FROM
        ts2.avl_data avl
        INNER JOIN 
        may2018_gtfs.stops s1 
        ON 
        CAST(avl.DepartureStopNumber AS STRING ) = s1.stop_code
        INNER JOIN 
        may2018_gtfs.stops s2
        ON 
        CAST( avl.ArrivalStopNumber AS STRING ) =  s2.stop_code 
        INNER JOIN 
        may2018_gtfs.stop_times st1 
        ON 
        avl.TripId = CAST( st1.trip_id AS INT64  ) 
        AND 
        s1.stop_id = CAST( st1.stop_id AS INT64  ) 
        INNER JOIN 
        may2018_gtfs.stop_times st2 
        ON 
        avl.TripId = CAST( st2.trip_id AS INT64 ) 
        AND 
        s2.stop_id = CAST( st2.stop_id AS INT64 ) 
  ) 
) 
WHERE 
  stop2stop_tt_sec <> 0 
;
 
/* 4.0 On Time Performance */ 
-- Each row in the avl data represents a departure and arrival, which may adhere to the schedule. Let's summarise these overall.  
--Apply a bin label based on the scheduled adherence. 

SELECT 
  d_grades.departure_adherence_grade AS adherence_grade, 
  (d_grades.grade_count + a_grades.grade_count ) / (d_grades.count_total + a_grades.count_total ) * 100.0 AS total_grade_percent, 
  d_grades.grade_percent AS departure_only_perc , 
  a_grades.grade_percent AS arrival_only_perc
FROM 
  (  
      SELECT 
        departure_adherence_grade, COUNT(*) AS grade_count, ( SELECT COUNT(*) FROM ts2.avl_data ) AS count_total, COUNT(*) / ( SELECT COUNT(*) FROM ts2.avl_data ) * 100 as grade_percent
      FROM 
        ( 
          SELECT 
            *,
            CASE 
              WHEN ScheduleAdherence_DepartureStop_Seconds_ BETWEEN -60 AND 180 THEN 'GRADE A' -- 1 min early to 3 min late
              WHEN ScheduleAdherence_DepartureStop_Seconds_ BETWEEN -120 AND 360 THEN 'GRADE B' -- 2 min early to 6 min late 
              WHEN ScheduleAdherence_DepartureStop_Seconds_ BETWEEN -240 AND 720 THEN 'GRADE C' -- 4 min early to 12 min late
              ELSE 'GRADE D'
            END AS departure_adherence_grade
          FROM 
            ts2.avl_data
        ) 
      GROUP BY 
        departure_adherence_grade
  ) d_grades 
  INNER JOIN 
  (
    SELECT 
      arrival_adherence_grade, COUNT(*) AS grade_count, ( SELECT COUNT(*) FROM ts2.avl_data ) AS count_total, COUNT(*) / ( SELECT COUNT(*) FROM ts2.avl_data ) * 100 as grade_percent
    FROM 
      ( 
        SELECT 
          *, 
          CASE 
            WHEN ScheduleAdherence_ArrivalStop_Seconds_ BETWEEN -60 AND 180 THEN 'GRADE A' -- 1 min early to 3 min late
            WHEN ScheduleAdherence_ArrivalStop_Seconds_ BETWEEN -120 AND 360 THEN 'GRADE B' -- 2 min early to 6 min late 
            WHEN ScheduleAdherence_ArrivalStop_Seconds_ BETWEEN -240 AND 720 THEN 'GRADE C' -- 4 min early to 12 min late
            ELSE 'GRADE D' 
          END AS arrival_adherence_grade 
        FROM 
          ts2.avl_data
      ) 
    GROUP BY 
      arrival_adherence_grade
  ) a_grades 
  ON 
  d_grades.departure_adherence_grade = a_grades.arrival_adherence_grade



