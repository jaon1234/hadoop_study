-- start transaction;
-- -- 插入客户到数据库中
-- insert into customers(cust_name) values('transaction test');
-- -- 保留点
-- savepoint delete1 ;
-- -- 插入客户的订单
-- select last_insert_id() into @new_id;
-- insert into orders(order_date,cust_id) values(now(),@new_id);
-- -- 订单明细
-- select last_insert_id() into @new_id;
-- insert into orderitems(order_num,order_item,prod_id,quantity,item_price)
-- values (@new_id,1,'FC',10,55),(@new_id,2,'TNT2',10,10);
-- savepoint delete2;
-- rollback to delete1;
-- commit;

create procedure tran_test(
    out result int
)
begin
    -- 声明回滚的标志符号，在事务中遇到执行异常的之后回滚
    declare do_roll int default 0;
    -- select 1 into do_roll;
    -- 在continue handler中，捕获sql异常，若发生，将do_roll设置为1
    declare continue handler for sqlexception set do_roll = 1;

    -- 下面开始事务
    start transaction;
        insert into customers(cust_id,cust_name)
        values(10020,'test_transaction_1');
        -- 让其主键相同，会执行失败
        insert into customers(cust_id,cust_name)
        values(10020,'test_transaction_2');
        -- 如果发生了异常，执行事务的回滚
    if do_roll=1 then
        rollback;
    else
        commit;
    end if;

    select do_roll into result;
end 