# hadoop 实战
hadoop是现在最流行，生态系统最完善的分布式数据处理框架（framework）。本篇文档主要讲述的是*如何快速部署hadoop，并让hadoop运行起来*，并不打算详细解释hadoop的工作原理。如果对hadoop的运行原理有兴趣，推荐阅读**《hadoop权威指南》**。
## hadoop是什么
本质上来讲，hadoop就是一堆java库文件，这些库文件之间相互配合，实现了一个分布式计算框架。程序员在hadoop之上开发，可以将工作重心转移至如何实现算法，避免了陷入机器之间的任务调度，数据依赖，进程同步等细枝末节的泥潭之中。
从功能上来区分，可以将hadoop划分为以下三个模块

- HDFS(hadoop distributed file system)
这是hadoop分布式文件系统。分布式文件系统就是将不同电脑的硬盘统一管理，在逻辑上被当做一个硬盘。用户将数据存储至HDFS中，可以不必关心具体数据存储在哪一台计算机当中，而且存入的数据都会做一定数量（可配置）的备份。就算某一台电脑硬盘损坏，也不会发生数据丢失的惨剧。
HDFS中有一台电脑地位比较高，称作namenode。namenode不存储用户的数据，只是记录了文件系统的metadata，即文件目录结构，具体的数据存储指向等等。用户往HDFS中读取或存储数据都需要通过namenode来操作。HDFS中的其他电脑都称之为datanode，用户的数据都被切割成小块（block）存储在这些datanode中，datanode由namenode来管理。
- hadoop Yarn
如果说HDFS是对多台计算机存储资源的抽象，那么hadoop Yarn是对多台计算机计算资源的抽象。为了实现对各个计算机计算资源的管理，和HDFS类似，yarn也定义了一台电脑为管理者，管理者电脑运行resource manager，其他电脑则运行node manager。node manager收集本电脑的资源耗费情况（CPU，内存，硬盘，网络 ），并定时向resource manager汇报。resource manager则依据各个节点的空闲资源来进行任务调度。
- map-reduce
如果只能用一个词来描述hadoop的运行原理，那么一定就是map-reduce了。在进行hadoop开发的过程中，90%的时间都是在考虑如何将算法过程描述为map reduce过程。正是因为有了map reduce抽象，算法才能自动并行的由resource manager来调度运行。map-reduce的原理比较抽象，三言两语难以解释清楚。要想理解map reduce，必须结合着实际例子来进行。我还是建议大家看完**《hadoop权威指南》**前几章之后，直接运行一个例子来理解这个话题。
## 前置技能要求
要具体开发一个hadoop程序，必须有以下两点的知识。

- linux基础知识
一般来讲hadoop运行在服务器端，服务器端的主流操作系统都是Linux。hadoop的部分功能实现依赖了linux脚本，因此，linux系统是运行hadoop的最佳（几乎唯一）选择。如果不熟悉linux，推荐看**《鸟哥的linux私房菜-基础学习篇》**。看完之后多加练习就可以了。
- java
hadoop是用java编写的framework，要求会java语言就不必说了。市场上的java入门书汗牛充栋，随便挑一本即可。
## 如何部署hadoop
hadoop是一个分布式处理框架，自然是要部署在多台电脑上的。按照hadoop的架构，至少要有两台电脑，一台是master，一台是slave。如果有N台电脑，那么其中1台是master，剩下的N-1台都是slaves。master电脑上运行着namenode和resource manager, slaves电脑则运行datanode和node manager。大部分情况下，我们都是通过在master上来访问并控制slaves的。
由于master和slaves分工不同，在参数配置上也会有所区别。首先讲master和slaves的共同配置，最后再分别讲master和slaves参数不同的地方。
### master和slaves的共同配置
- 安装linux
所有电脑都要安装linux，推荐使用linux的Ubuntu发行版，使用的人多，遇到问题了网上也容易搜到。
- 安装jvm(java virtual marchine)
jvm是java的运行环境。所有java程序都必须运行于jvm之上，hadoop自然也不例外。jvm有很多不同的实现，可以通过[hadoop wiki](https://wiki.apache.org/hadoop/HadoopJavaVersions)来查看jvm的兼容性。
Ubuntu发行版应该自带了openjdk，我们用它就够了。通过在bash(linux的命令行工具)中键入：`java --version`，如果能返回版本号，说明电脑就已经安装好了jvm。如果没有返回，那么输入下面的命令来安装。
`sudo apt-get install openjdk-8-jdk`
jvm的安装路径一般为/usr/lib/jvm/jvm_name
- 安装hadoop
去[hadoop发布页面](http://hadoop.apache.org/releases.html)下载最新的hadoop程序包，注意下载binary版本。假设下载文件名字位：hadoop-X.Y.Z.tar.gz  在bash中键入
`tar -zxvf hadoop.tar.gz -C /usr/local/`
- 建立hadoop用户群
为了方面后边配置和管理，建议建立单独的hadoop用户账号。在bash中依次输入
``` bash
sudo addgroup hadoop   
sudo adduser --ingroup hadoop hduser
sudo chown -R hduser:hadoop /usr/local/hadoop-X.Y.Z
``` 
记住这一步设置的用户密码,后面用得到
- 设置环境变量
hadoop的运行依赖于一些系统参数，因此我们要事先设置一些环境变量。
首先切换到hadoop账户下,
`su - hadoop`
修改/home/hadoop/.bashrc文件，在文件末尾
``` bash
# 添加hadoop变量
export HADOOP_HOME=/usr/local/hadoop
# Set JAVA_HOME (we will also configure JAVA_HOME directly for Hadoop later on)
export JAVA_HOME=/usr/lib/jvm/java-6-sun
# Add Hadoop bin/ directory to PATH
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```
- 修改hosts文件
按如下格式修改/etc/hosts文件
格式: ip  hostname
```
192.168.1.43 slave01(hostname)
192.168.1.41 master
192.168.1.42 slave02
```
注意:这里的hostname一定是在bash中运行`hostname`返回的字符串
此外如果有hosts文件中有`127.0.1.1 hostname`或者 `127.0.0.1 hostname`这样的行，则一定要删掉，不然会影响hadoop网络之间的通信。
- 安装ssh-server
hadoop通过ssh来管理各个节点，ubuntu默认已经安装了ssh访问程序，但是还要对每台电脑还要安装服务器端程序。
`sudo apt-get install openssh-server`

###只需要对master进行的设置
- 设置ssh免密码访问
生成本地sshkey
`ssh-keygen -t rsa -P  ''`
密码一定要是空的，这样才能实现无密码登录。
为了实现master对slaves的免密码访问，将master的public key添加到slave的authorized_keys中
`ssh-copy-id -i $HOME/.ssh/id_rsa.pub hduser@slave`
此外还要将master的public key添加到master的authorized_keys，因为namenode在本机上，也要ssh登录的。
`cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys`
当访问某个host时，会有是否将该host加入known host的提问，答应就好了
- 设置hadoop系统参数
以下的参数设置默认目录是/usr/local/hadoop
将etc/hadoop/hadoop-env.sh文件中的$JAVA_HOME改为/usr/lib/jvm/java-8-oracle

修改etc/hadoop/slaves文件，添加各个datanode的ip 地址
slave01
slave02

hdfs-site.xml中设置namenode的存储路径和datanode的存储路径。
```xml
<property>
<name>dfs.replication</name>
<value>2</value>
</property>
<property>
<name>dfs.namenode.name.dir</name>
<value>/hadoop-data/hadoopuser/hdfs/namenode</value>
</property>
<property>
<name>dfs.datanode.data.dir</name>
<value>/hadoop-data/hadoopuser/hdfs/datanode</value>
</property>
```
给master分配namenode的存储空间（不需要data.dir）
给slaves分配datanode的存储空间(不需要name.dir)

core-site.xml 设置hdfs
```xml
<property>
<name>fs.defaultFS</name>
<value>hdfs://master:54310</value>
</property>
```

yarn-site.xml设置map reduce工作类型
```xml
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
<property>
<name>yarn.resourcemanager.hostname</name>
<value>master</value>
</property>
```
设置完毕之后，通过rsync将修改后的hadoop配置文件同步到各个slaves上
`sudo rsync -avxP /usr/local/hadoop/ hduser@HadoopSlave1:/usr/local/hadoop/`
#### 配置文件的一些解释
- core-site.xml
其中fs.defaultFS的value中的host就是namenode，所以，不需要单独设定namenode了。
- hdfs-site.xml
dfs.namenode.name.dir的value是存储namenode数据的地方，这个value可以用逗号分割开，不同逗号之间的目录分别存储一套namenode meta data，起到备份的作用。
dfs.datanode.data.dir的value是hdfs存储data block的地方，这个value也可以用逗号分割开，但是每个目录都用来存储数据，不备份。
dfs.replication表示存储在hdfs中的数据做几个备份，默认是3。当然如果只有2台电脑做datanode，那就最多只能设置为2了。

- 在浏览器中输入`namenode:50070/explorer.html`来查看HDFS的运行状态


我之前已经在实验室的三台电脑上搭建了一个hadoop cluster，一台为master，剩余两台为slaves。大家可以去实地操作体验一下，其实没什么复杂的，多练练，很快就能上手。
我之前还写过一个基于hadoop的文本聚类程序，大家可以通过这个[链接](https://github.com/galoiscode/ChineseArticleCluster)来查看源码，从中可以体验一下map reduce到底是什么，怎样才能把算法MR化。
<div align = right>written by wangshuang
2015.05.10</div>







