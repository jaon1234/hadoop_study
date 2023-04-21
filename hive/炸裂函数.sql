-- 根据上述电影信息表，统计各分类的电影数量
-- 将电影的分类进行切割
with a as (
    select
        movie,
        split(category,",") as cates
    from movie_info
), b as(
    select
        movie,
        cate
    from a
    lateral view explode(cates) tmp as cate
)
select cate,count(*)
from b
group by cate;