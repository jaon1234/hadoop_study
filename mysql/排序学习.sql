-- ----------------------------
-- Table structure for Department
-- ----------------------------
DROP TABLE IF EXISTS `Department`;
CREATE TABLE `Department` (
  `Id` int(11) NOT NULL,
  `Name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 
-- ----------------------------
-- Records of Department
-- ----------------------------
INSERT INTO `Department` VALUES ('1', 'IT');
INSERT INTO `Department` VALUES ('2', 'Sales');
INSERT INTO `Department` VALUES ('3', 'free');
 
-- ----------------------------
-- Table structure for Employee
-- ----------------------------
DROP TABLE IF EXISTS `Employee`;
CREATE TABLE `Employee` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Name` varchar(255) DEFAULT NULL,
  `Salary` int(11) DEFAULT NULL,
  `DepartmentId` int(11) DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;
 
-- ----------------------------
-- Records of Employee
-- ----------------------------
INSERT INTO `Employee` VALUES ('1', 'Joe', '70000', '1');
INSERT INTO `Employee` VALUES ('2', 'Henry', '80000', '2');
INSERT INTO `Employee` VALUES ('3', 'Sam', '60000', '2');
INSERT INTO `Employee` VALUES ('4', 'Max', '90000', '1');
INSERT INTO `Employee` VALUES ('5', 'Janet', '85000', '1');
INSERT INTO `Employee` VALUES ('6', 'Randy', '85000', '1');
INSERT INTO `Employee` VALUES ('7', 'hshs', '4000000', '1');
INSERT INTO `Employee` VALUES ('8', 'ces', '60000', '2');


-- 按照group by排序，
SELECT   
    emp.id,emp.name,emp.salary,dep.id as depId,dep.name as depName  
FROM   
    Employee as emp,Department as dep,Employee as emp1
WHERE
    emp.salary <= emp1.salary
    AND emp.departmentId = emp1.departmentId
    and emp.departmentId = dep.id   
GROUP BY   
    dep.id ,emp.id,emp.name ,emp.salary,dep.name  
HAVING COUNT(*)<=3  
ORDER BY   
    emp.departmentId , emp.salary desc; 


SELECT   
    emp.id,emp.name,emp.salary,dep.id as depId,dep.name as depName  
FROM   
    Employee as emp,Department as dep,Employee as emp1
WHERE
    emp.salary <= emp1.salary
    and emp.departmentId = emp1.departmentId
    and emp.departmentId = dep.id   
GROUP BY   
    dep.id ,emp.id,emp.name ,emp.salary,dep.name  
HAVING COUNT(*)<=3  
ORDER BY   
    emp.departmentId , emp.salary desc;  

-- 自联结下的笛卡尔积
select e1.id,e1.name,e1.salary,dep.name,COUNT(*) as ranking
from Employee as e1,Employee as e2,Department as dep
where 
    e1.departmentId = e2.departmentId -- 这代表与部门内部的各个人建立笛尔卡积的联系
    and e1.salary <= e2.salary -- 保留他自己，以及比他薪水高的人的联系
    and e1.departmentId = dep.id
    -- order by e1.name,e2.salary;
    -- 这个时候观察数据，若是排名第一，则以其分组只有一条；
    -- 排名第二，以其分组则有两条；
GROUP by
    e1.id,e1.name,e1.salary,dep.name
-- count的数量是多少，就是排名多少
HAVING ranking <= 3
order by dep.name,ranking;

-- 窗口函数的排序学习，
-- 窗口函数可以理解为就是往原有查询结果上面增加了一列
-- 语法如下：
-- <窗口函数> over (partition by <用于分组的列名>
--                     order by <用于排序的列名>)
-- 窗口函数有两种：一种是专门的窗口函数，一种是聚合函数
-- 专用窗口函数1.rank，rank排序时候，若是分数相同，则名次也会一样;
select *,
rank()  over (
partition by DepartmentId
order by Salary desc) as ranking -- order by不要放在外面，因为这里是组内排序
from Employee
order by DepartmentId desc;
-- 下面考虑所有的专用窗口函数
select *,
rank()  over (order by Salary) as ranking,
dense_rank() over(order by Salary) as dense_ranking,
row_number() over (order by salary) as row_number_ranking
from Employee;
-- 下面试试聚合函数，聚合函数表示截止到当前行的聚合,若是加上了分组，则是分组内聚合
select *,
sum(Salary)  over (partition by DepartmentId order by Salary) as sum_sal,
avg(Salary) over(partition by DepartmentId  order by Salary) as avg_sal,
max(Salary) over (partition by DepartmentId  order by salary) as max_sal,
min(Salary) over (partition by DepartmentId  order by salary) as min_sal,
count(Salary) over (partition by DepartmentId  order by salary) as count_sal
from Employee;