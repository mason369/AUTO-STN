# AUTO-STN
自动化巡检

# 如何使用？

## 确保有Python环境
前往  Python 官网，下载并安装 Python 3.8 或更高版本。
安装完成后，在终端中执行以下命令确认是否安装成功。

```shell
python --version
```

若安装成功，终端中会输出已安装的 Python 的版本号。


## 2.安装外部包
```bash
pip install openpyxl pytz paramiko tqdm colorama
```

## 3.运行

```
Python engineerl-V2.6.py
```

# 文件说明
## host-stna.csv
若运行1~5的命令或专项巡检子功能中的前4个需使用到该文件需手动创建该文件，指定要执行的命令

示例
```
screen-length 512
show device
show temperature
show fan
show laser
show interface main
show ospf neighbor brief
show ldp session
show mpls l2vc brief
show bfd session brief
show alarm current
screen-length 25
```

## host-stna.csv
格式为：
```
只需替换设备IP，用户名，密码
x.x.x.x,xxx,xxxx,stna-cmd1.csv,stna-result1.txt,stna-result1.csv
1.1.1.1,admin,admin,stna-cmd1.csv,stna-result1.txt,stna-result1.csv
```

## userhost-stna.csv
若运行执行15.自定义指令功能需手动创建该文件，指定要执行的设备IP，用户名，密码，格式与host-stna.csv一样

## 自定义指令.txt
若运行执行15.自定义指令功能需手动创建该文件，指定要执行的命令

格式为：
```
con
show fan
show tem
fan speed percent 90
sa
```
