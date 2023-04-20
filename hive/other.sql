select
  job,
  if(sex='男',1,0) as male,
  if(sex='女',1,0) as female
from
  employee;

