-- mysql case
-- 语法一，判断条件中，只涉及到一列
case 列名
when 条件（>80）then 结果
else 结果
end
as 别名
-- 语法二，判断条件中，涉及到多列，多列时，相当于按照顺序对该行的值进行判断，有成功的则立即返回
case 
when 列名及条件(people>80) then 结果
else 结果
end
as 别名