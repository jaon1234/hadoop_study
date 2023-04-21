select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    sum(order_amount) over(partition by user_id order by order_date)
    as sum_so_far
from order_info;
-- 窗口函数可以在子句中添加下面的语句，控制窗口的行为：
-- rows between …… and ……
-- unbounded preceding 前面所有行
-- unbounded following 后面所有行
-- current row 当前行
-- n following  后面n行
-- n preceding  前面n行
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    sum(order_amount) 
    over(partition by user_id order by order_date
        rows between unbounded preceding and 1 following
    )
    as sum_so_far
from order_info;

-- 按月分组统计用户的累计下单金额
select 
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    sum(order_amount) over (
        partition by user_id,month(order_date)
        order by order_date
        rows between unbounded preceding and current row
    )
from order_info;

-- 统计用户每次下单距离上一次下单的间隔天数
-- 统计出上一次下单的时间
with a as (
    select
        order_id,
        user_id,
        user_name,
        order_date,
        order_amount,
        lag(order_date,1,null) over(
            partition by user_id
            order by order_date
        ) as last_order_date
    from   
        order_info
)
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    nvl(datediff(order_date,last_order_date),0) as diff
from a;

-- 查询所有下单记录以及每个用户的每个下单记录所在月份的首/末次下单日期
-- 查询每个月的首次及末次下单日期
with a as(
    select
        order_id,
        user_id,
        user_name,
        order_date,
        order_amount,
        first_value(order_date,false) over(
            partition by user_id,month(order_date)
            order by order_date
            rows between unbounded preceding and current row
        ) as first_order_date,
        last_value(order_date,false) over(
            partition by user_id,month(order_date)
            order by order_date
            rows between unbounded preceding and current row
        ) as last_order_date
    from order_info
)

-- 为每个用户的所有下单记录按照订单金额进行排名
select
    order_id,
    user_id,
    user_name,
    order_date,
    order_amount,
    rank() over (
        partition by user_id
        order by order_amount desc
    ) as ranking,
    dense_rank() over (
        partition by user_id
        order by order_amount desc
    ) as dense_ranking,
    row_number() over(
        partition by user_id
        order by order_amount desc
    ) as row_numbering
from order_info;
