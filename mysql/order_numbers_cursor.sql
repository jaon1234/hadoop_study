create procedure produceprocess()
begin
    -- 声明变量用于存储获取的结果
    declare o int;
    -- 标记是否停止循环，默认为0
    declare down boolean default 0;
    -- 声明变量用于存储总价
    declare t decimal(8,2);
    -- 声明游标
    declare ordernumbers cursor 
    for
        select order_num from orders;
    --  声明continue handler的停止条件，mysql查询不到下一条数据抛出错误代码的时候
    declare continue handler for sqlstate '02000' set down = 1;
    -- 创建一个新表，存储获取的总价格
    create table if not exists ordertotals(
        order_num int,
        total_price decimal(8,2)
    );
    -- 遍历获得所有的产品id，遍历并获取总价，存储到新表当中
    -- 打开游标
    open ordernumbers;
    repeat
        fetch ordernumbers into o;
        -- -- 调用另一个存储过程，获取总价
        call ordertotal(o,t);
        -- 插入到新表当中
        insert into ordertotals(
            order_num,total_price
        )values(
            o,t
        );
    until down end repeat;
    -- 关闭游标
    close ordernumbers;
end //
