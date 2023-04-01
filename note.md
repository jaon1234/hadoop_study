# shell脚本
## 一些常见的参数说明
- #!/bin/bash
- echo$0    # 当前脚本的文件名（间接运行时还包括绝对路径）。
- echo $n    # 传递给脚本或函数的参数。n 是一个数字，表示第几个参数。例如，第一个参数是 $1 。
- echo $#    # 传递给脚本或函数的参数个数。
- echo $*    # 传递给脚本或函数的所有参数。
- echo $@    # 传递给脚本或函数的所有参数。被双引号 (" ") 包含时，与 $* 不同，下面将会讲到。
- echo $?    # 上个命令的退出状态，或函数的返回值。
- echo $$    # 当前 Shell 进程 ID。对于 Shell 脚本，就是这些脚本所在的进程 ID。
- echo $_    # 上一个命令的最后一个参数
- echo $!    # 后台运行的最后一个进程的 ID 
## 关于$
- $ 后面跟一个变量名，可直接获取该变
- $() 表示执行括号中的内容，并且返回，例如：

  <code>$(cd -P $(dirname $file); pwd)</code>

- 上面就表示先获取 当前文件的目录，再根据-P获取文件的真实路径并打开，打开后执行pwd，返回路径

# hadoop的总结
## 1、常用端口号
    hadoop3.x 
		HDFS NameNode 内部通常端口：8020/9000/9820
		HDFS NameNode 对用户的查询端口：9870
		Yarn查看任务运行情况的：8088
		历史服务器：19888
    
    hadoop2.x 
		HDFS NameNode 内部通常端口：8020/9000
		HDFS NameNode 对用户的查询端口：50070
		Yarn查看任务运行情况的：8088
		历史服务器：19888
        
## 2、常用的配置文件
	3.x core-site.xml  hdfs-site.xml  yarn-site.xml  mapred-site.xml workers
	2.x core-site.xml  hdfs-site.xml  yarn-site.xml  mapred-site.xml slaves