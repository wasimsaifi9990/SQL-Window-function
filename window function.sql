
use wasimdb
select * from employee;
show tables;

-- max salary earn by employee in each department
select e.* , max(salary) 
over(partition by dept_name) as max_salary 
from employee as e 


-- Row Number
select * from (
	select e.* ,row_number() over(partition by dept_name order by emp_id ) as rn
	from employee as e
) as x
where x.rn<3


-- fetch the top 3 employee in each dept earning the max salary.
select * from (
	select e.* ,  rank() over(partition by dept_name order by salary desc ) as rk
	from employee as e
) as x
where x.rk<3

-- Dense rank
select e.* ,
rank() over(partition by dept_name order by salary desc) as rnk,
dense_rank() over(partition by dept_name order by salary desc) as den_rnk,
row_number() over(partition by dept_name)
from employee e 

-- lead and lag
-- fetch a query to display if the salary of an employee is higher, lower or equal to the previous employee.



select e.*,
lag(salary) over(partition by dept_name order by emp_id ) as prev_empl_sal,
case 
when e.salary > lag(salary) over(partition by dept_name order by emp_id )  then 'Higher then previous'
when e.salary < lag(salary) over (partition by dept_name order by emp_id) then 'Lower then previous'
when e.salary = lag(salary) over (partition by dept_name order by emp_id) then 'equal to  previous'
end as salary_range
from employee as e

--                   PART 2
select * from product;

-- FIRST_VALUE 
-- Write query to display the most expensive product under each category (corresponding to each record)
select p.* ,
first_value(product_name) over(partition by product_category order by price desc) as most_expensive_product
from product p;

-- LAST_VALUE 
-- Write query to display the least expensive product under each category (corresponding to each record)
select *,
first_value(product_name) over(partition by product_category order by price desc) as Most_expensive ,
last_value(product_name) over(partition by product_category order by price desc range between unbounded preceding and unbounded following) 
as least_expensive 
from product as p
where product_category = 'Phone';


select p.*,
first_value(product_name) over w as Most_expensive ,
last_value(product_name) over w as least_expensive 
from product as p
where product_category = 'Phone'
window w as (partition by product_category order by price desc 
range between unbounded preceding and unbounded following)

select p.*,
first_value(product_name) over w as Most_expensive ,
last_value(product_name) over w as least_expensive 
from product as p
where product_category = 'Phone'
window w as (partition by product_category order by price desc 
range between unbounded preceding and current row)


-- nth_value
-- Write query to display the Second most expensive product under each category.
select *,
nth_value(product_name,2) over w as nd_expensive
from product
window w as (partition by product_category order by price desc
	range between unbounded preceding and unbounded following);

-- NTILE
-- Write a query to segregate all the expensive phones, mid range phones and the cheaper phones.

/* Ntile syntax
	NTILE(n) OVER (
    PARTITION BY <expression>[{,<expression>...}]
    ORDER BY <expression> [ASC|DESC], [{,<expression>...}]
)
*/
select *,
	case 
		when x.busket = 1 then 'most_expensive'
        when x.busket = 2 then 'mid range'
        when x.busket = 3 then 'least expensive' 
        end as buket from
        (
		select * ,
		ntile(3) over(order by price desc) as busket
		from product 
		order by busket,price desc
		) as x
        
-- CUME_DIST (cumulative distribution) ; 
/*  Formula = Current Row no (or Row No with value same as current row) / Total no of rows */

-- Query to fetch all products which are constituting the first 30% 
-- of the data in products table based on price.


select product_name
from(
	select * ,
	cume_dist() over(order by price desc) as cum_dist,
	round(cume_dist() over(order by price desc)*100 ,2) as cume_dist_round
	from product ) as x
where x.cume_dist_round <30

select product_name,per_rank from (
	select *,
	round(percent_rank() over(order by price )*100,2) as per_rank
	from product) as x
where x.product_name = 'Galaxy Z Flip 3'