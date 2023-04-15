create procedure productpricing_01(
    in onumber int,
    in taxable boolean,
    out ototal decimal(8,2)
) comment 'product pricing caculate.'
begin
    --  相当于声明局部变量
    declare total decimal(8,2);
    declare taxrate int default 6;
    -- 计算不含税价格
    select sum(item_price*quantity)
    from orderitems
    where order_num = onumber
    into total;
    --  计算含税价格，if taxable is 1
    if taxable then
        select total+(total/100*taxrate)  into ototal;
    end if;
    select total into ototal;
end//

