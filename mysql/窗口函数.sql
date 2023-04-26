rows between 2 preceding and current row # 取本行和前面两行

rows between unbounded preceding and current row # 取本行和之前所有的行 

rows between current row and unbounded following # 取本行和之后所有的行 

rows between 3 preceding and 1 following # 从前面三行和下面一行，总共五行 

-- # 当order by后面没有rows between时，窗口规范默认是取本行和之前所有的行

-- # 当order by和rows between都没有时，窗口规范默认是分组下所有行（rows between unbounded preceding and unbounded following）

-- -- rows between 分为：之前、当前、之后，三个部分，之前使用  数字 preceding ，当前 current row 之后使用 1 preceding,最前面和最后面，则通过将关键字替换为 unbounded
-- 窗口函数的三个专有函数:rank()、dense_rank()、row_numbers()

-- 当前数据的上方数据lag，当前数据的下方数据lead，参数分别是:列名，offset，超出记录窗口时的默认值