-- Databricks notebook source
select * from f1_presentation.calculated_results

-- COMMAND ----------

select driver_name ,count (1) as total_races,  sum (calculated_points) as calculated_points, 
avg (calculated_points) as avg_points
from f1_presentation.calculated_results
where driver_name like '%ontoya%'
group by driver_name
having total_races>=50
order by avg_points desc

-- COMMAND ----------

