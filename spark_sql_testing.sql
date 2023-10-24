-- Databricks notebook source
select *from employee1_csv

-- COMMAND ----------

select sum(salary) as sal from employee1_csv

-- COMMAND ----------

-- MAGIC %md ##Select Table

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md ##Filter Table

-- COMMAND ----------

select * from employee1_csv where salary > 10000

-- COMMAND ----------

-- MAGIC %md ##Sorting

-- COMMAND ----------

select * from employee1_csv where salary > 10000 order by name desc

-- COMMAND ----------

-- MAGIC %md ##Aggregated --> sum,min,max,avg

-- COMMAND ----------

select max(salary) from employee1_csv

-- COMMAND ----------

-- MAGIC %md ##grouping

-- COMMAND ----------

select sum(salary),location from employee1_csv 
group by location
order by sum(salary) desc

-- COMMAND ----------

-- MAGIC %md  CTE (Each SQL CTE is like a named query, whose result is stored in a virtual table(CTE) to be referenced later in the main query)

-- COMMAND ----------

with CTE as (
  select *, dense_rank() over(order by salary desc) as rnk from employee1_csv
)
select *from CTE where rnk =3

-- COMMAND ----------

-- MAGIC %md Window Function --row_number,rank,dense_rank

-- COMMAND ----------

select *,row_number() over(order by name desc) as rw from employee1_csv

-- COMMAND ----------

select *,rank() over(order by salary asc) as rnk  from employee1_csv

-- COMMAND ----------

select *,dense_rank() over(order by salary desc) as rnk  from employee1_csv

-- COMMAND ----------

-- MAGIC %md ##Remove Duplicates

-- COMMAND ----------

WITH CTE AS (
select *,dense_rank() over(order by salary desc) as rnk  from employee1_csv
)

Select *from CTE where rnk = 4

-- COMMAND ----------

-- MAGIC %md ###VIEW

-- COMMAND ----------

create or replace temp view remove_duplicates as 
select *,dense_rank() over(order by salary desc) as rnk  from employee1_csv

-- COMMAND ----------

select *from remove_duplicates

-- COMMAND ----------

-- MAGIC %md ##Permanent View
-- MAGIC

-- COMMAND ----------

-- MAGIC %md Temparory view we can call it on only in the present notebook
-- MAGIC

-- COMMAND ----------

-- MAGIC %md Permanent view we can call it anywhere in the cluster

-- COMMAND ----------

create or replace view remove_duplicates as 
select *,dense_rank() over(order by salary desc) as rnk  from employee1_csv

-- COMMAND ----------

-- MAGIC %md ##Create Table

-- COMMAND ----------

 create or replace table new_table as
 select * from employee1_csv
 

-- COMMAND ----------

select * from new_table

-- COMMAND ----------

 
