create trigger new_product after insert on products
for each row select 'Product added' into @tag;

create trigger deleteorder before delete on orders
for each row
begin
    insert into archive_orders(order_num,order_date,cust_id)
    values(OLD.order_num,OLD.order_date,OLD.cust_id);
end //

