with a as(
    select name,
           replace(birthday,"/","-") as birthday
    from employee
)
, b as (
    select name,
           year(current_date())-year(a.birthday) as year,
           month(current_date())-month(a.birthday) as month
    from a
)

select
    name,
    concat(if(month>=0,year,year-1),"年",
           if(month>=0,month,12+month),"月"
    ) as age
from
    b